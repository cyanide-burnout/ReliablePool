#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/select.h>
#include <linux/limits.h>
#include <systemd/sd-daemon.h>

#include "Rescue.h"
#include "HashMap.h"

#ifdef USE_LIGHTNOTIFIER
#include "LightNotifier.h"
#endif

#define MAP_FD    0
#define MAP_NAME  1

#define FLAG_SOURCE_SYSTEM   (1 << 0)
#define FLAG_RELEASE_NOTIFY  (1 << 1)
#define FLAG_RELEASE_CLOSE   (1 << 2)
#define FLAG_RELEASE_NAME    (1 << 3)

struct RescueRecord
{
  int handle;
  int length;
  char* name;
  atomic_uint_fast32_t flags;
};

static pthread_rwlock_t lock = PTHREAD_RWLOCK_INITIALIZER;
static struct HashMap* maps[] = { NULL, NULL };

static char* GetToken(char** data, int* length)
{
  char* value;

  *length = 0;
  value   = *data;

  if (*data != NULL)
  {
    while ((**data != ':') &&
           (**data != '\0'))
    {
      (*data)   ++;
      (*length) ++;
    }

    (*data) += (**data == ':');
  }

  return value;
}

static void StoreRescueRecord(struct RescueRecord* record)
{
  if ((record->length < 0)   ||
      (record->name == NULL) ||
      (PutIntoHashMap(maps[MAP_FD],   &record->handle, sizeof(int),    record) != HASHMAP_SUCCESS) ||
      (PutIntoHashMap(maps[MAP_NAME],  record->name,   record->length, record) != HASHMAP_SUCCESS))
  {
    // Memory allocation error
    abort();
  }
}

static void ReleaseRescueRecord(void* key, void* data)
{
  struct RescueRecord* record;
  char buffer[PATH_MAX];
  uint32_t flags;

  record = (struct RescueRecord*)data;
  flags  = atomic_load_explicit(&record->flags, memory_order_acquire);

  RemoveFromHashMap(maps[MAP_NAME], record->name, record->length, record);

  if (flags & FLAG_RELEASE_NOTIFY)
  {
    snprintf(buffer, PATH_MAX, "FDSTOREREMOVE=1\nFDNAME=%.*s\n", record->length, record->name);
    sd_pid_notify_with_fds(0, 0, buffer, NULL, 0);
  }

  if (flags & FLAG_RELEASE_CLOSE)
  {
    // Close when unused
    close(record->handle);
  }

  if (flags & FLAG_RELEASE_NAME)
  {
    // Release when owned
    free(record->name);
  }

  free(record);
}

static int HandleRescueRecord(void* key, size_t size, void* data, void* argument1, void* argument2)
{
  HandleRescuedHandle function;
  struct RescueRecord* record;
  int condition;
  int result;

  record    = (struct RescueRecord*)data;
  function  = (HandleRescuedHandle)argument1;
  condition = atomic_load_explicit(&record->flags, memory_order_relaxed) & FLAG_SOURCE_SYSTEM;
  result    = function(record->handle, record->name, condition, argument2);

  switch (result)
  {
    case RESCUE_REMOVE_CLOSE:  atomic_fetch_or_explicit(&record->flags, FLAG_RELEASE_NOTIFY, memory_order_relaxed);                           return 1;
    case RESCUE_REMOVE_SAVE:   atomic_fetch_and_explicit(&record->flags, ~(FLAG_RELEASE_NOTIFY | FLAG_RELEASE_CLOSE), memory_order_relaxed);  return 1;
    case RESCUE_KEEP_ACQUIRE:  atomic_fetch_and_explicit(&record->flags, ~(FLAG_RELEASE_NOTIFY | FLAG_RELEASE_CLOSE), memory_order_relaxed);  return 0;
  }

  return 0;
}

static int FilterRescueRecord(void* key, size_t size, void* data, void* argument1, void* argument2)
{
  struct RescueRecord* record;
  uint32_t flags;

  record = (struct RescueRecord*)data;
  flags  = atomic_load_explicit(&record->flags, memory_order_relaxed);

  return (flags & FLAG_SOURCE_SYSTEM) &&
         (flags & FLAG_RELEASE_CLOSE);
}

static void __attribute__((constructor(105))) Initialize()  // Should be initialized AFTER CRC32C
{
  int count;
  int handle;
  int length;
  char* name;
  char* data;

  struct RescueRecord* record;

  if ((getppid() == 1) &&
      (sd_booted() > 0) &&
      (getenv("NOTIFY_SOCKET") != NULL))
  {
    maps[MAP_FD]   = CreateHashMap(ReleaseRescueRecord);
    maps[MAP_NAME] = CreateHashMap(NULL);

    count  = sd_listen_fds(0);
    handle = SD_LISTEN_FDS_START;
    data   = getenv("LISTEN_FDNAMES");

    while ((count  > 0) &&
           (handle < FD_SETSIZE) &&
           (name   = GetToken(&data, &length)))
    {
      record         = (struct RescueRecord*)calloc(1, sizeof(struct RescueRecord));
      record->handle = fcntl(handle, F_DUPFD_CLOEXEC, FD_SETSIZE);

      if (record->handle < 0)
      {
        // Relocation failed
        goto Continue;
      }

      record->length = length;
      record->name   = name;

      close(handle);
      atomic_store_explicit(&record->flags, FLAG_SOURCE_SYSTEM | FLAG_RELEASE_NOTIFY | FLAG_RELEASE_CLOSE, memory_order_relaxed);
      StoreRescueRecord(record);

      handle ++;
      count  --;
    }

    while ((count > 0) &&
           (name  = GetToken(&data, &length)))
    {
      record = (struct RescueRecord*)calloc(1, sizeof(struct RescueRecord));

      Continue:

      record->handle = handle;
      record->length = length;
      record->name   = name;

      fcntl(handle, F_SETFD, FD_CLOEXEC);
      atomic_store_explicit(&record->flags, FLAG_SOURCE_SYSTEM | FLAG_RELEASE_NOTIFY | FLAG_RELEASE_CLOSE, memory_order_relaxed);
      StoreRescueRecord(record);

      handle ++;
      count  --;
    }

    unsetenv("LISTEN_FDS");
    unsetenv("LISTEN_PID");
  }
}

static void __attribute__((destructor)) Release()
{
  ReleaseHashMap(maps[MAP_FD]);
  ReleaseHashMap(maps[MAP_NAME]);
}

void AddRescuedHandle(int handle, const char* format, ...)
{
  va_list arguments;
  char buffer[PATH_MAX];
  struct RescueRecord* record;

  if ((handle >= 0) &&
      (*maps != NULL))
  {
    record         = (struct RescueRecord*)calloc(1, sizeof(struct RescueRecord));
    record->handle = handle;

    if (strchr(format, '%') != NULL)
    {
      va_start(arguments, format);
      record->length = vasprintf(&record->name, format, arguments);
      atomic_store_explicit(&record->flags, FLAG_RELEASE_NAME, memory_order_relaxed);
      va_end(arguments);
    }
    else
    {
      record->length = strlen(format);
      record->name   = (char*)format;
    }

    snprintf(buffer, PATH_MAX, "FDSTORE=1\nFDNAME=%.*s\n", record->length, record->name);

    pthread_rwlock_wrlock(&lock);
    StoreRescueRecord(record);
    pthread_rwlock_unlock(&lock);

    sd_pid_notify_with_fds(0, 0, buffer, &handle, 1);
  }
}

void RemoveRescuedHandle(int handle, int action)
{
  struct RescueRecord* record;
  char buffer[PATH_MAX];

  if ((handle >= 0) &&
      (*maps != NULL))
  {
    buffer[0] = '\0';

    pthread_rwlock_wrlock(&lock);
    if (GetFromHashMap(maps[MAP_FD], &handle, sizeof(int), (void**)&record) == HASHMAP_SUCCESS)
    {
      if (action == RESCUE_REMOVE_CLOSE)
      {
        // Prepare a message, notification is placed outside the lock to minimize lock time
        snprintf(buffer, PATH_MAX, "FDSTOREREMOVE=1\nFDNAME=%.*s\n", record->length, record->name);
      }

      // Remove FLAG_RELEASE_NOTIFY due to unwanted or manual notification
      atomic_fetch_and_explicit(&record->flags, ~FLAG_RELEASE_NOTIFY, memory_order_relaxed);
      RemoveFromHashMap(maps[MAP_FD], &record->handle, sizeof(int), record);
    }
    pthread_rwlock_unlock(&lock);

    if (buffer[0] != '\0')
    {
      // Finally send the message if prepared
      sd_pid_notify_with_fds(0, 0, buffer, NULL, 0);
    }
  }
}

void IterateRescuedHandleList(HandleRescuedHandle function, void* closure)
{
  if (*maps != NULL)
  {
    pthread_rwlock_wrlock(&lock);
    IterateThroughHashMap(maps[MAP_FD], HandleRescueRecord, function, closure);
    pthread_rwlock_unlock(&lock);
  }
}

int GetRescuedHandle(const char* format, ...)
{
  int handle;
  int length;
  va_list arguments;
  char buffer[PATH_MAX];
  struct RescueRecord* record;

  handle = -1;

  if (*maps != NULL)
  {
    if (strchr(format, '%') != NULL)
    {
      va_start(arguments, format);
      length = vsnprintf(buffer, PATH_MAX, format, arguments);
      format = buffer;
      va_end(arguments);
    }
    else
    {
      // Format has exact name
      length = strlen(format);
    }

    pthread_rwlock_rdlock(&lock);
    if (GetFromHashMap(maps[MAP_NAME], format, length, (void**)&record) == HASHMAP_SUCCESS)
    {
      handle = record->handle;
      atomic_fetch_and_explicit(&record->flags, ~(FLAG_RELEASE_NOTIFY | FLAG_RELEASE_CLOSE), memory_order_relaxed);
    }
    pthread_rwlock_unlock(&lock);
  }

  return handle;
}

int GetRescuedCount()
{
  return (*maps != NULL) ? (maps[MAP_FD]->length) : -1;
}

void CloseUnusedRescuedHandleList()
{
  if (*maps != NULL)
  {
    pthread_rwlock_wrlock(&lock);
    IterateThroughHashMap(maps[MAP_FD], FilterRescueRecord, NULL, NULL);
    pthread_rwlock_unlock(&lock);
  }
}
