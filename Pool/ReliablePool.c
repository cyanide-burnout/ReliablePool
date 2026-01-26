#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "ReliablePool.h"

#define GRANULARITY  1024

_Static_assert((offsetof(struct ReliableBlock, next)  % sizeof(uint64_t))      == 0, "ReliableBlock.next must be 64-bit aligned");
_Static_assert((offsetof(struct ReliableBlock, mark)  % sizeof(uint64_t))      == 0, "ReliableBlock.mark must be 64-bit aligned");
_Static_assert((offsetof(struct ReliableBlock, data)  % __BIGGEST_ALIGNMENT__) == 0, "RReliableBlock.data must be aligned to __BIGGEST_ALIGNMENT__");
_Static_assert((offsetof(struct ReliableMemory, free) % sizeof(uint64_t))      == 0, "ReliableMemory.free must be 64-bit aligned");
_Static_assert((offsetof(struct ReliableMemory, data) % __BIGGEST_ALIGNMENT__) == 0, "ReliableMemory.data must be aligned to __BIGGEST_ALIGNMENT__");

static inline int AcquireBlock(ATOMIC(uint32_t)* counter)
{
  uint32_t value;

  value = atomic_load_explicit(counter, memory_order_acquire);

  while (value != 0)
    if (atomic_compare_exchange_weak_explicit(counter, &value, value + 1, memory_order_acq_rel, memory_order_acquire))
      return 0;

  return -1;
}

static inline void PushFreeBlock(struct ReliableMemory* memory, struct ReliableBlock* block)
{
  uint64_t current;
  uint64_t next;

  next = ((uint64_t)atomic_fetch_add_explicit(&block->tag, 1, memory_order_relaxed) << 32) | block->number;

  do
  {
    current = atomic_load_explicit(&memory->free, memory_order_relaxed);
    atomic_store_explicit(&block->next, current, memory_order_relaxed);
  }
  while (!atomic_compare_exchange_weak_explicit(&memory->free, &current, next, memory_order_release, memory_order_relaxed));
}

static inline struct ReliableBlock* PopFreeBlock(struct ReliableMemory* memory)
{
  uint64_t current;
  uint64_t next;
  uint32_t number;
  struct ReliableBlock* block;

  do
  {
    current = atomic_load_explicit(&memory->free, memory_order_acquire);
    number  = (uint32_t)current;

    if (number == UINT32_MAX)
    {
      block = NULL;
      break;
    }

    block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
    next  = atomic_load_explicit(&block->next, memory_order_relaxed);
  }
  while (!atomic_compare_exchange_weak_explicit(&memory->free, &current, next, memory_order_acq_rel, memory_order_relaxed));

  return block;
}

static inline int CheckShareCapacity(struct ReliableShare* share, uint32_t number)
{
  struct ReliableMemory* memory;
  uint32_t length;

  memory = share->memory;
  length = (uint32_t)((share->size - offsetof(struct ReliableMemory, data)) / memory->size);

  return -((number != UINT32_MAX) && (number >= length));
}

static inline int CheckPoolCapacity(struct ReliablePool* pool)
{
  struct ReliableMemory* memory;
  struct ReliableShare* share;
  uint32_t number;

  share  = pool->share;
  memory = share->memory;
  number = (uint32_t)atomic_load_explicit(&memory->free, memory_order_acquire);

  return CheckShareCapacity(share, number);
}

static inline int TruncateFile(int handle, off_t length)
{
  int result;

  do result = ftruncate(handle, length);
  while ((result < 0) && (errno == EINTR));

  return result;
}

static int LockFileWrite(int handle)
{
  struct flock lock;
  int result;

  memset(&lock, 0, sizeof(struct flock));

  lock.l_type   = F_WRLCK;
  lock.l_whence = SEEK_SET;

  do result = fcntl(handle, F_OFD_SETLKW, &lock);
  while ((result < 0) && (errno == EINTR));

  return result;
}

static int LockFileRead(int handle)
{
  struct flock lock;
  int result;

  memset(&lock, 0, sizeof(struct flock));

  lock.l_type   = F_RDLCK;
  lock.l_whence = SEEK_SET;

  do result = fcntl(handle, F_OFD_SETLKW, &lock);
  while ((result < 0) && (errno == EINTR));

  return result;
}

static int UnlockFile(int handle)
{
  struct flock lock;
  int result;

  memset(&lock, 0, sizeof(struct flock));

  lock.l_type   = F_UNLCK;
  lock.l_whence = SEEK_SET;

  do result = fcntl(handle, F_OFD_SETLK, &lock);
  while ((result < 0) && (errno == EINTR));

  return result;
}

static void ReleaseShare(struct ReliablePool* pool, struct ReliableShare* share, uint64_t weight)
{
  uint64_t result;

  result = atomic_fetch_sub_explicit(&share->weight, weight, memory_order_acq_rel) - weight;

  if ((result <  RELIABLE_WEIGHT_STRONG) &&
      (weight == RELIABLE_WEIGHT_STRONG) &&
      (pool   != NULL))
  {
    // Call monitor only when the last RELIABLE_WEIGHT_STRONG is released
    CallReliableMonitor(RELIABLE_MONITOR_SHARE_DESTROY, pool, share, NULL);
  }

  if (result == 0ULL)
  {
    munmap(share->memory, share->size);
    free(share);
  }
}

static void ReleasePool(struct ReliablePool* pool)
{
  if (atomic_fetch_sub_explicit(&pool->count, 1, memory_order_acq_rel) == 1)
  {
    ReleaseShare(NULL, pool->share, RELIABLE_WEIGHT_STRONG);
    pthread_rwlock_destroy(&pool->lock);
    close(pool->handle);
    free(pool);
  }
}

static struct ReliablePool* CreateNewMemory(int handle, const char* name, size_t size, size_t length, uint32_t flags, struct ReliableMonitor* monitor)
{
  uint32_t number;
  struct ReliablePool* pool;
  struct ReliableShare* share;
  struct ReliableBlock* block;
  struct ReliableMemory* memory;

  if ((TruncateFile(handle, size) < 0) ||
      ((memory = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, handle, 0)) == MAP_FAILED))
  {
    // An error occurred
    return NULL;
  }

  number         = (size - sizeof(struct ReliableMemory)) / length;
  memory->magic  = RELIABLE_MEMORY_MAGIC;
  memory->size   = length;

  atomic_store_explicit(&memory->length, number, memory_order_relaxed);
  atomic_store_explicit(&memory->free, (uint64_t)UINT32_MAX, memory_order_relaxed);
  strncpy(memory->name, name, RELIABLE_MEMORY_NAME_LENGTH);

  pool   = (struct ReliablePool*)calloc(1, sizeof(struct ReliablePool));
  share  = (struct ReliableShare*)calloc(1, sizeof(struct ReliableShare));

  if ((pool  == NULL) ||
      (share == NULL))
  {
    free(pool);
    free(share);
    munmap(memory, size);
    return NULL;
  }

  pool->share   = share;
  pool->handle  = handle;
  share->memory = memory;
  share->size   = size;

  atomic_store_explicit(&pool->monitor, monitor,                memory_order_relaxed);
  atomic_store_explicit(&pool->count,   1,                      memory_order_relaxed);
  atomic_store_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  while ((number --) > 0)
  {
    block         = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
    block->number = number;
    PushFreeBlock(memory, block);
  }

  CallReliableMonitor(RELIABLE_MONITOR_POOL_CREATE, pool, share, NULL);
  return pool;
}

static struct ReliablePool* UseExistingMemory(int handle, const char* name, size_t size, size_t length, uint32_t flags, struct ReliableMonitor* monitor, ReliableRecoveryFunction function, void* closure)
{
  uint32_t number;
  struct ReliablePool* pool;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  if ((size < sizeof(struct ReliableMemory)) ||
      ((memory = (struct ReliableMemory*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, handle, 0)) == MAP_FAILED))
  {
    // An error occurred
    return NULL;
  }

  if ((memory->size  != length) ||
      (memory->magic != RELIABLE_MEMORY_MAGIC) ||
      (strncmp(memory->name, name, RELIABLE_MEMORY_NAME_LENGTH) != 0))
  {
    // Wrong data format
    munmap(memory, size);
    return NULL;
  }

  pool   = (struct ReliablePool*)calloc(1, sizeof(struct ReliablePool));
  share  = (struct ReliableShare*)calloc(1, sizeof(struct ReliableShare));

  if ((pool  == NULL) ||
      (share == NULL))
  {
    free(pool);
    free(share);
    munmap(memory, size);
    return NULL;
  }

  pool->share   = share;
  pool->handle  = handle;
  share->memory = memory;
  share->size   = size;

  atomic_store_explicit(&pool->monitor, monitor,                memory_order_relaxed);
  atomic_store_explicit(&pool->count,   1,                      memory_order_relaxed);
  atomic_store_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  CallReliableMonitor(RELIABLE_MONITOR_POOL_CREATE, pool, share, NULL);

  if (flags & RELIABLE_FLAG_RESET)
  {
    for (number = 0; number < atomic_load_explicit(&memory->length, memory_order_relaxed); ++ number)
    {
      block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

      if (block->type != RELIABLE_TYPE_FREE)
      {
        atomic_store_explicit(&block->count, 0, memory_order_relaxed);

        if ((function != NULL) &&
            (block->type == RELIABLE_TYPE_RECOVERABLE) &&
            (block->type  = function(pool, block, closure)))
        {
          CallReliableMonitor(RELIABLE_MONITOR_BLOCK_RECOVER, pool, share, block);
          continue;
        }

        block->type = RELIABLE_TYPE_FREE;
        uuid_clear(block->identifier);
        atomic_store_explicit(&block->mark,    0, memory_order_relaxed);
        atomic_store_explicit(&block->control, 0, memory_order_relaxed);
        PushFreeBlock(memory, block);
      }
    }
  }

  return pool;
}

static int ExpandMemory(struct ReliablePool* pool)
{
  size_t size;
  uint32_t length;
  uint32_t number;
  struct stat status;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  pthread_rwlock_wrlock(&pool->lock);

  if (LockFileWrite(pool->handle) < 0)
  {
    pthread_rwlock_unlock(&pool->lock);
    return -1;
  }

  if (fstat(pool->handle, &status) < 0)
  {
    UnlockFile(pool->handle);
    pthread_rwlock_unlock(&pool->lock);
    return -1;
  }

  share  = pool->share;
  memory = share->memory;
  size   = status.st_size + pool->grain;
  length = (size - sizeof(struct ReliableMemory)) / memory->size;

  if (((share  = (struct ReliableShare*)calloc(1, sizeof(struct ReliableShare))) == NULL) ||
      (TruncateFile(pool->handle, size) < 0) ||
      ((memory = (struct ReliableMemory*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->handle, 0)) == MAP_FAILED))
  {
    UnlockFile(pool->handle);
    pthread_rwlock_unlock(&pool->lock);
    free(share);
    return -1;
  }

  ReleaseShare(pool, share, RELIABLE_WEIGHT_STRONG);

  share->memory = memory;
  share->size   = size;
  pool->share   = share;

  atomic_store_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  for (number = atomic_load_explicit(&memory->length, memory_order_relaxed); number < length; ++ number)
  {
    block         = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
    block->number = number;
    PushFreeBlock(memory, block);
  }

  atomic_store_explicit(&memory->length, length, memory_order_release);

  UnlockFile(pool->handle);
  pthread_rwlock_unlock(&pool->lock);

  CallReliableMonitor(RELIABLE_MONITOR_SHARE_CREATE, pool, share, NULL);

  return 0;
}

struct ReliablePool* CreateReliablePool(int handle, const char* name, size_t length, uint32_t flags, struct ReliableMonitor* monitor, ReliableRecoveryFunction function, void* closure)
{
  size_t grain;
  struct stat status;
  struct ReliablePool* pool;

  if (LockFileWrite(handle) == 0)
  {
    length += sizeof(struct ReliableBlock);
    length  = (length + __BIGGEST_ALIGNMENT__ - 1) & ~(__BIGGEST_ALIGNMENT__ - 1);
    grain   = length * GRANULARITY;
    grain   = (grain + getpagesize() - 1) & ~(getpagesize() - 1);

    if ((fstat(handle, &status) == 0) &&
        ((pool = UseExistingMemory(handle, name, status.st_size, length, flags, monitor, function, closure)) ||
         (pool = CreateNewMemory(handle, name, grain, length, flags, monitor))))
    {
      pool->grain = grain;
      pool->time  = status.st_ctime;

      pthread_rwlock_init(&pool->lock, NULL);
      UnlockFile(handle);
      return pool;
    }

    UnlockFile(handle);
  }

  return NULL;
}

void ReleaseReliablePool(struct ReliablePool* pool)
{
  struct ReliableShare* share;

  if (pool != NULL)
  {
    CallReliableMonitor(RELIABLE_MONITOR_POOL_RELEASE, pool, pool->share, NULL);
    ReleasePool(pool);
  }
}

int UpdateReliablePool(struct ReliablePool* pool)
{
  int result;
  struct stat status;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  pthread_rwlock_wrlock(&pool->lock);

  if (LockFileRead(pool->handle) < 0)
  {
    result = - errno;
    pthread_rwlock_unlock(&pool->lock);
    return result;
  }

  if (fstat(pool->handle, &status) < 0)
  {
    result = - errno;
    UnlockFile(pool->handle);
    pthread_rwlock_unlock(&pool->lock);
    return result;
  }

  if ((share = pool->share) &&
      (share->size >= status.st_size))
  {
    UnlockFile(pool->handle);
    pthread_rwlock_unlock(&pool->lock);
    return 0;
  }

  if (((share  = (struct ReliableShare*)calloc(1, sizeof(struct ReliableShare)))                                          == NULL) ||
      ((memory = (struct ReliableMemory*)mmap(NULL, status.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->handle, 0)) == MAP_FAILED))
  {
    UnlockFile(pool->handle);
    pthread_rwlock_unlock(&pool->lock);
    free(share);
    return -ENOMEM;
  }

  ReleaseShare(pool, share, RELIABLE_WEIGHT_STRONG);

  pool->share   = share;
  share->memory = memory;
  share->size   = status.st_size;

  atomic_store_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  UnlockFile(pool->handle);
  pthread_rwlock_unlock(&pool->lock);

  CallReliableMonitor(RELIABLE_MONITOR_SHARE_CREATE, pool, share, NULL);

  return 1;
}

void* AllocateReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, int type)
{
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  for (block = NULL; block == NULL; )
  {
    pthread_rwlock_rdlock(&pool->lock);

    if (CheckPoolCapacity(pool) < 0)
    {
      pthread_rwlock_unlock(&pool->lock);
      UpdateReliablePool(pool);
      continue;
    }

    share  = pool->share;
    memory = share->memory;
    block  = PopFreeBlock(memory);

    if (block != NULL)
    {
      atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);
      atomic_fetch_add_explicit(&pool->count,   1,                      memory_order_relaxed);
      pthread_rwlock_unlock(&pool->lock);

      block->type   = type;
      block->length = memory->size - offsetof(struct ReliableBlock, data);

      atomic_store_explicit(&block->count, 1, memory_order_relaxed);
      memset(block->data, 0, block->length);

      CallReliableMonitor(RELIABLE_MONITOR_BLOCK_ALLOCATE, pool, share, block);

      descriptor->pool  = pool;
      descriptor->share = share;
      descriptor->block = block;

      return block->data;
    }

    pthread_rwlock_unlock(&pool->lock);

    if (ExpandMemory(pool) < 0)
    {
      descriptor->pool  = NULL;
      descriptor->share = NULL;
      descriptor->block = NULL;
      break;
    }
  }

  return NULL;
}

void* AttachReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, uint32_t number, uint32_t tag)
{
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  descriptor->pool  = NULL;
  descriptor->share = NULL;
  descriptor->block = NULL;

  pthread_rwlock_rdlock(&pool->lock);

  if (CheckShareCapacity(pool->share, number) < 0)
  {
    pthread_rwlock_unlock(&pool->lock);
    UpdateReliablePool(pool);
    pthread_rwlock_rdlock(&pool->lock);
  }

  share  = pool->share;
  memory = share->memory;

  if ((CheckShareCapacity(share, number) < 0) ||
      (number >= atomic_load_explicit(&memory->length, memory_order_acquire)))
  {
    pthread_rwlock_unlock(&pool->lock);
    return NULL;
  }

  block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

  if ((tag != atomic_load_explicit(&block->tag, memory_order_acquire)) ||
      (AcquireBlock(&block->count) < 0))
  {
    pthread_rwlock_unlock(&pool->lock);
    return NULL;
  }

  atomic_fetch_add_explicit(&pool->count,   1,                      memory_order_relaxed);
  atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  descriptor->pool  = pool;
  descriptor->share = share;
  descriptor->block = block;

  pthread_rwlock_unlock(&pool->lock);

  CallReliableMonitor(RELIABLE_MONITOR_BLOCK_ATTACH, pool, share, block);

  return block->data;
}

void* ShareReliableBlock(const struct ReliableDescriptor* source, struct ReliableDescriptor* destination)
{
  struct ReliablePool* pool;
  struct ReliableShare* share;
  struct ReliableBlock* block;

  if (source->pool != NULL)
  {
    pool  = destination->pool  = source->pool;
    share = destination->share = source->share;
    block = destination->block = source->block;

    atomic_fetch_add_explicit(&pool->count,   1,                      memory_order_relaxed);
    atomic_fetch_add_explicit(&block->count,  1,                      memory_order_relaxed);
    atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);
    return block->data;
  }

  destination->pool  = NULL;
  destination->share = NULL;
  destination->block = NULL;
  return NULL;
}

void ReleaseReliableBlock(struct ReliableDescriptor* descriptor, int type)
{
  struct ReliablePool* pool;
  struct ReliableBlock* block;
  struct ReliableShare* share;

  if (descriptor->pool != NULL)
  {
    pool  = descriptor->pool;
    share = descriptor->share;
    block = descriptor->block;

    if (atomic_fetch_sub_explicit(&block->count, 1, memory_order_acquire) == 1)
    {
      block->type = type;

      CallReliableMonitor(RELIABLE_MONITOR_BLOCK_RELEASE, pool, share, block);

      if (type == RELIABLE_TYPE_FREE)
      {
        uuid_clear(block->identifier);
        atomic_store_explicit(&block->mark,    0, memory_order_relaxed);
        atomic_store_explicit(&block->control, 0, memory_order_relaxed);
        PushFreeBlock(share->memory, block);
      }
    }

    ReleaseShare(pool, share, RELIABLE_WEIGHT_STRONG);
    ReleasePool(pool);

    descriptor->pool  = NULL;
    descriptor->share = NULL;
    descriptor->block = NULL;
  }
}

void* RecoverReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, struct ReliableBlock* block)
{
  struct ReliableShare* share;

  share = pool->share;

  descriptor->pool  = pool;
  descriptor->share = share;
  descriptor->block = block;

  atomic_fetch_add_explicit(&pool->count,   1,                      memory_order_relaxed);
  atomic_fetch_add_explicit(&block->count,  1,                      memory_order_relaxed);
  atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);

  return block->data;
}

uint32_t ReserveReliableBlock(struct ReliablePool* pool, uuid_t identifier, int type)
{
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  for (block = NULL; block == NULL; )
  {
    pthread_rwlock_rdlock(&pool->lock);

    if (CheckPoolCapacity(pool) < 0)
    {
      pthread_rwlock_unlock(&pool->lock);
      UpdateReliablePool(pool);
      continue;
    }

    share  = pool->share;
    memory = share->memory;
    block  = PopFreeBlock(memory);

    if (block != NULL)
    {
      atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);
      pthread_rwlock_unlock(&pool->lock);

      block->type   = (uint32_t)type;
      block->length = memory->size - offsetof(struct ReliableBlock, data);

      atomic_store_explicit(&block->count,   0, memory_order_relaxed);
      atomic_store_explicit(&block->mark,    0, memory_order_relaxed);
      atomic_store_explicit(&block->control, 0, memory_order_relaxed);
      uuid_copy(block->identifier, identifier);
      memset(block->data, 0, block->length);

      CallReliableMonitor(RELIABLE_MONITOR_BLOCK_RESERVE, pool, share, block);

      ReleaseShare(pool, share, RELIABLE_WEIGHT_STRONG);

      return block->number;
    }

    pthread_rwlock_unlock(&pool->lock);

    if (ExpandMemory(pool) < 0)
    {
      // Error expanding the pool
      break;
    }
  }

  return UINT32_MAX;
}

int FreeReliableBlock(struct ReliablePool* pool, uint32_t number, uuid_t identifier)
{
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  pthread_rwlock_rdlock(&pool->lock);

  if (CheckShareCapacity(pool->share, number) < 0)
  {
    pthread_rwlock_unlock(&pool->lock);
    UpdateReliablePool(pool);
    pthread_rwlock_rdlock(&pool->lock);
  }

  share  = pool->share;
  memory = share->memory;

  if ((CheckShareCapacity(share, number) < 0) ||
      (number >= atomic_load_explicit(&memory->length, memory_order_acquire)))
  {
    pthread_rwlock_unlock(&pool->lock);
    return -ENOENT;
  }

  block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

  if ((block->type == RELIABLE_TYPE_FREE) ||
      (uuid_is_null(identifier) == 0)     &&
      (uuid_compare(identifier, block->identifier) != 0))
  {
    pthread_rwlock_unlock(&pool->lock);
    return -ENOENT;
  }

  if (atomic_load_explicit(&block->count, memory_order_acquire) != 0)
  {
    pthread_rwlock_unlock(&pool->lock);
    return -EBUSY;
  }

  block->type = RELIABLE_TYPE_FREE;

  atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_STRONG, memory_order_relaxed);
  pthread_rwlock_unlock(&pool->lock);

  CallReliableMonitor(RELIABLE_MONITOR_BLOCK_FREE, pool, share, block);

  uuid_clear(block->identifier);
  atomic_store_explicit(&block->mark,    0, memory_order_relaxed);
  atomic_store_explicit(&block->control, 0, memory_order_relaxed);

  PushFreeBlock(memory, block);
  ReleaseShare(pool, share, RELIABLE_WEIGHT_STRONG);

  return 0;
}

struct ReliableShare* MakeReliableShareCopy(struct ReliablePool* pool, struct ReliableShare* share)
{
  struct ReliableShare* result;
  struct ReliableMemory* memory;

  if (((result = (struct ReliableShare*)calloc(1, sizeof(struct ReliableShare))) == NULL) ||
      ((memory = (struct ReliableMemory*)mmap(NULL, share->size, PROT_READ | PROT_WRITE, MAP_SHARED, pool->handle, 0)) == MAP_FAILED))
  {
    free(result);
    return NULL;
  }

  result->size   = share->size;
  result->memory = memory;

  atomic_store_explicit(&result->weight, RELIABLE_WEIGHT_WEAK, memory_order_relaxed);

  return result;
}

void ReleaseReliableShare(struct ReliableShare* share)
{
  if (share != NULL)
  {
    // Call private implementation
    ReleaseShare(NULL, share, RELIABLE_WEIGHT_WEAK);
  }
}

void CallReliableMonitor(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block)
{
  struct ReliableMonitor* monitor;

  monitor = atomic_load_explicit(&pool->monitor, memory_order_relaxed);

  if (event == RELIABLE_MONITOR_POOL_RELEASE)
  {
    // RELIABLE_MONITOR_POOL_RELEASE should be a last message received by monitor
    monitor = atomic_exchange_explicit(&pool->monitor, NULL, memory_order_relaxed);
  }

  while (monitor != NULL)
  {
    monitor->function(event, pool, share, block, monitor->closure);
    monitor = monitor->next;
  }
}
