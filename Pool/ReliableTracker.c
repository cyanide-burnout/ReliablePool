#include "ReliableTracker.h"

#include <poll.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <systemd/sd-id128.h>

#include "CRC32C.h"

static inline int CallIOCTL(int handle, unsigned long code, void* parameter)
{
  int result;

  do result = ioctl(handle, code, parameter);
  while ((result < 0) && ((errno == EINTR) || (errno == EAGAIN)));

  return result;
}

static inline uint64_t MakeEpoch(struct ReliableTracker* tracker)
{
  uint64_t last;
  uint64_t number;
  uint64_t result;
  struct timespec time;

  clock_gettime(CLOCK_REALTIME, &time);

  last   = atomic_load_explicit(&tracker->last, memory_order_relaxed);
  result =
    (((uint64_t)time.tv_sec * 1000000000ULL + (uint64_t)time.tv_nsec) & ~0xffffffULL) |
    ((uint64_t)(tracker->node & UINT16_MAX) << 8) |
    ((uint64_t)(atomic_fetch_add_explicit(&tracker->epoch, 2, memory_order_relaxed) & UINT8_MAX));

  do
  {
    if (result <= last)
    {
      number  = (last + 2) & UINT8_MAX;
      result  = (last & ~UINT8_MAX) | number;
      result += (number == 0) * 0x1000000;
    }
  }
  while (!atomic_compare_exchange_weak_explicit(&tracker->last, &last, result, memory_order_release, memory_order_relaxed));

  return result;
}

static inline void WakeListner(struct ReliableTracker* tracker)
{
  if (~atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_KICK, memory_order_release) & RELIABLE_TRACKER_STATE_KICK)
  {
    // Alternative (and probably main) way to wake listener in parallel with RELIABLE_MONITOR_SHARE_CHANGE
    while ((syscall(SYS_futex, (uint32_t*)&tracker->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, 1, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
           (errno == EINTR));
  }
}

static void AddShareCopy(struct ReliableTracker* tracker, struct ReliablePool* pool, struct ReliableShare* share)
{
  struct ReliableShare* old;
  struct ReliableShare* new;

  if (new = MakeReliableShareCopy(pool, share))
  {
    pthread_rwlock_wrlock(&tracker->lock);
    old               = (struct ReliableShare*)pool->closures[0];
    pool->closures[0] = new;
    pthread_rwlock_unlock(&tracker->lock);

    RetireReliableShare(old);
    return;
  }

  atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
}

static void RemoveShareCopy(struct ReliableTracker* tracker, struct ReliablePool* pool)
{
  struct ReliableShare* share;

  pthread_rwlock_wrlock(&tracker->lock);
  share             = (struct ReliableShare*)pool->closures[0];
  pool->closures[0] = NULL;
  pthread_rwlock_unlock(&tracker->lock);

  RetireReliableShare(share);
}

static void AddTrackable(struct ReliableTracker* tracker, struct ReliablePool* pool, struct ReliableShare* share)
{
  struct uffdio_writeprotect protection;
  struct uffdio_register registration;
  struct ReliableTrackable* trackable;
  struct ReliableTrackable* other;
  size_t length;

  memset(&registration, 0, sizeof(struct uffdio_register));
  memset(&protection,   0, sizeof(struct uffdio_writeprotect));

  registration.range.start = (uintptr_t)share->memory;
  registration.range.len   = (share->size + tracker->size - 1) & ~(tracker->size - 1);
  length                   = (registration.range.len / tracker->size + 63) >> 6;

  if (trackable = (struct ReliableTrackable*)calloc(1, sizeof(struct ReliableTrackable) + length * sizeof(TrackablePageMap)))
  {
    trackable->pool    = pool;
    trackable->share   = share;
    trackable->length  = length;
    trackable->tracker = tracker;
    trackable->range   = registration.range;

    registration.mode  = UFFDIO_REGISTER_MODE_WP;
    protection.mode    = UFFDIO_WRITEPROTECT_MODE_WP;
    protection.range   = registration.range;

    if (CallIOCTL(tracker->handle, UFFDIO_REGISTER, &registration) < 0)
    {
      atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
      free(trackable);
      return;
    }

    if (CallIOCTL(tracker->handle, UFFDIO_WRITEPROTECT, &protection) < 0)
    {
      atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
      CallIOCTL(tracker->handle, UFFDIO_UNREGISTER, &registration.range);
      free(trackable);
      return;
    }

    atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_WEAK, memory_order_relaxed);

    pthread_rwlock_wrlock(&tracker->lock);
    if (other = tracker->list)
    {
      trackable->next = other;
      other->previous = trackable;
    }

    tracker->list      = trackable;
    share->closures[0] = trackable;
    pthread_rwlock_unlock(&tracker->lock);
    return;
  }

  atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
}

static void RemoveTrackable(struct ReliableTracker* tracker, struct ReliablePool* pool, struct ReliableShare* share)
{
  struct ReliableTrackable* trackable;
  struct ReliableTrackable* other;
  int result;

  if (trackable = (struct ReliableTrackable*)share->closures[0])
  {
    share->closures[0] = NULL;
    result             = CallIOCTL(tracker->handle, UFFDIO_UNREGISTER, &trackable->range);
    atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE * (result < 0), memory_order_relaxed);

    pthread_rwlock_wrlock(&tracker->lock);
    if (other = trackable->next)      other->previous = trackable->previous;
    if (other = trackable->previous)  other->next     = trackable->next;
    else                              tracker->list   = trackable->next;
    pthread_rwlock_unlock(&tracker->lock);

    RetireReliableShare(share);
    free(trackable);
  }
}

static void RemoveAllTrackable(struct ReliableTracker* tracker, struct ReliablePool* pool)
{
  struct ReliableTrackable* trackable;
  struct ReliableTrackable* previous;
  struct ReliableTrackable* next;
  struct ReliableTrackable* list;
  struct ReliableShare* share;
  int result;

  list = NULL;

  pthread_rwlock_wrlock(&tracker->lock);

  for (trackable = tracker->list; trackable != NULL; trackable = next)
  {
    next = trackable->next;

    if (trackable->pool == pool)
    {
      previous           = trackable->previous;
      share              = trackable->share;
      share->closures[0] = NULL;

      if (next     != NULL) next->previous = previous;
      if (previous != NULL) previous->next = next;
      else                  tracker->list  = next;

      trackable->next = list;
      list            = trackable;
    }
  }

  pthread_rwlock_unlock(&tracker->lock);

  for (trackable = list; trackable != NULL; trackable = next)
  {
    next   = trackable->next;
    result = CallIOCTL(tracker->handle, UFFDIO_UNREGISTER, &trackable->range);
    atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE * (result < 0), memory_order_relaxed);

    RetireReliableShare(trackable->share);
    free(trackable);
  }
}

static int HandleFaultyPage(struct ReliableTracker* tracker, uintptr_t address)
{
  struct ReliableTrackable* trackable;
  size_t page;
  int result;

  result = -1;

  pthread_rwlock_rdlock(&tracker->lock);

  for (trackable = tracker->list; trackable != NULL; trackable = trackable->next)
  {
    if ((address >= (uintptr_t)(trackable->range.start)) &&
        (address <  (uintptr_t)(trackable->range.start + trackable->range.len)))
    {
      page = (address - (uintptr_t)trackable->range.start) / tracker->size;
      atomic_fetch_or_explicit(trackable->map + (page >> 6), 1ULL << (page & 63), memory_order_relaxed);
      atomic_fetch_or_explicit(&trackable->state, RELIABLE_TRACKABLE_STATE_DURTY, memory_order_relaxed);

      WakeListner(tracker);
      CallReliableMonitor(RELIABLE_MONITOR_SHARE_CHANGE, trackable->pool, trackable->share, NULL);
      result = ~atomic_load_explicit(&trackable->state, memory_order_relaxed) & RELIABLE_TRACKABLE_STATE_LOCK;
      break;
    }
  }

  pthread_rwlock_unlock(&tracker->lock);

  return result;
}

static void HandleDurtyBlock(struct ReliableTracker* tracker, struct ReliableTrackable* trackable, struct ReliableBlock* block, uint64_t epoch)
{
  struct ReliableShare* share;
  struct ReliablePool* pool;
  uint32_t control;

  if ((epoch != atomic_load_explicit(&block->mark, memory_order_acquire)) &&
      (block->type != RELIABLE_TYPE_FREE))
  {
    control = GetCRC32C(block->data, block->length, 0);

    if (control != atomic_load_explicit(&block->control, memory_order_relaxed))
    {
      atomic_store_explicit(&block->control, control, memory_order_relaxed);
      atomic_store_explicit(&block->mark,    epoch,   memory_order_release);
      atomic_store_explicit(&block->hint,    epoch,   memory_order_release);

      pool  = trackable->pool;
      share = (struct ReliableShare*)pool->closures[0];

      CallReliableMonitor(RELIABLE_MONITOR_BLOCK_CHANGE, pool, share, block);
      return;
    }

    if (atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_FLAG_FORCE_MARK)
    {
      atomic_store_explicit(&block->mark, epoch, memory_order_relaxed);
      return;
    }
  }
}

static void HandleDurtyPage(struct ReliableTracker* tracker, struct ReliableTrackable* trackable, uintptr_t page, uint64_t epoch)
{
  struct ReliableMemory* memory;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  uintptr_t start;
  uintptr_t end;
  uint32_t last;
  uint32_t limit;
  uint32_t number;

  pool  = trackable->pool;
  share = (struct ReliableShare*)pool->closures[0];

  start = page * tracker->size + (page == 0ULL) * offsetof(struct ReliableMemory, data);
  end   = (page + 1ULL) * tracker->size - 1ULL;

  if ((share != NULL) &&
      (share->size > start))
  {
    memory = share->memory;
    limit  = atomic_load_explicit(&memory->length, memory_order_acquire);
    number = (uint32_t)((start - offsetof(struct ReliableMemory, data)) / memory->size);
    last   = (uint32_t)((end   - offsetof(struct ReliableMemory, data)) / memory->size);

    while ((number <= last) &&
           (number <  limit))
    {
      block = (struct ReliableBlock*)(memory->data + memory->size * (size_t)(number ++));
      HandleDurtyBlock(tracker, trackable, block, epoch);
    }
  }
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct ReliableTracker* tracker;

  tracker = (struct ReliableTracker*)closure;

  if (atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
  {
    switch (event)
    {
      case RELIABLE_MONITOR_POOL_CREATE:
      case RELIABLE_MONITOR_SHARE_CREATE:
        AddShareCopy(tracker, pool, share);
        AddTrackable(tracker, pool, share);
        break;

      case RELIABLE_MONITOR_POOL_RELEASE:
        RemoveAllTrackable(tracker, pool);
        RemoveShareCopy(tracker, pool);
        break;
    }
  }
}

static void* DoWork(void* closure)
{
  struct ReliableTracker* tracker;
  struct uffdio_writeprotect protection;
  struct uffd_msg message;
  struct pollfd event;
  ssize_t result;

  tracker = (struct ReliableTracker*)closure;

  memset(&protection, 0, sizeof(struct uffdio_writeprotect));

  event.fd      = tracker->handle;
  event.events  = POLLIN;
  event.revents = 0;

  protection.mode      = 0;
  protection.range.len = tracker->size;

  pthread_setname_np(tracker->thread, "Tracker");

  while (atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
  {
    if ((poll(&event, 1, 200) > 0) &&
        (event.revents & POLLIN))
    {
      event.revents = 0;
      result        = read(tracker->handle, &message, sizeof(struct uffd_msg));
      if ((result == sizeof(struct uffd_msg)) &&
          (message.event == UFFD_EVENT_PAGEFAULT) &&
          (message.arg.pagefault.flags & UFFD_PAGEFAULT_FLAG_WP))
      {
        protection.range.start = (uintptr_t)message.arg.pagefault.address & ~(uintptr_t)(tracker->size - 1);
        if ((HandleFaultyPage(tracker, protection.range.start) != 0) &&
            (CallIOCTL(tracker->handle, UFFDIO_WRITEPROTECT, &protection) < 0))
        {
          // Unlock failed
          atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
        }
      }
    }
  }

  return NULL;
}

struct ReliableTracker* CreateReliableTracker(uint32_t flags, struct ReliableMonitor* next)
{
  struct ReliableTracker* tracker;
  struct uffdio_api interface;
  sd_id128_t identifier;
  pid_t process;

  if (tracker = (struct ReliableTracker*)calloc(1, sizeof(struct ReliableTracker)))
  {
    sd_id128_get_machine(&identifier);

    process                 = getpid();
    tracker->super.next     = next;
    tracker->super.closure  = tracker;
    tracker->super.function = HandleMonitorEvent;

    if (flags & RELIABLE_TRACKER_FLAG_ID_HOST)     tracker->node = GetCRC32C((uint8_t*)&identifier, sizeof(sd_id128_t), tracker->node);
    if (flags & RELIABLE_TRACKER_FLAG_ID_PROCESS)  tracker->node = GetCRC32C((uint8_t*)&process, sizeof(pid_t), tracker->node);
    if (flags & RELIABLE_TRACKER_FLAG_ID_UNIQUE)   tracker->node = GetCRC32C((uint8_t*)&tracker, sizeof(void*), tracker->node);

    pthread_rwlock_init(&tracker->lock, NULL);

    memset(&interface, 0, sizeof(struct uffdio_api));

    tracker->handle    = syscall(SYS_userfaultfd, O_CLOEXEC | O_NONBLOCK);
    tracker->size      = sysconf(_SC_PAGESIZE);
    interface.api      = UFFD_API;
    interface.features = UFFD_FEATURE_PAGEFAULT_FLAG_WP;

    if ((tracker->handle >= 0) &&
        (CallIOCTL(tracker->handle, UFFDIO_API, &interface) == 0) &&
        (interface.features & UFFD_FEATURE_PAGEFAULT_FLAG_WP))
    {
      atomic_store_explicit(&tracker->state, (flags & ~UINT16_MAX) | RELIABLE_TRACKER_STATE_ACTIVE, memory_order_relaxed);
      if (pthread_create(&tracker->thread, NULL, DoWork, tracker) != 0)
      {
        // Startup error, the contents of thread are undefined
        atomic_store_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE, memory_order_relaxed);
      }
    }
  }

  return tracker;
}

void ReleaseReliableTracker(struct ReliableTracker* tracker)
{
  struct ReliableTrackable* trackable;

  if (tracker != NULL)
  {
    if (atomic_fetch_and_explicit(&tracker->state, ~RELIABLE_TRACKER_STATE_ACTIVE, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
    {
      // Thread might be not started
      pthread_join(tracker->thread, NULL);
    }

    while (trackable = tracker->list)
    {
      tracker->list = trackable->next;

      CallIOCTL(tracker->handle, UFFDIO_UNREGISTER, &trackable->range);
      RetireReliableShare(trackable->share);
      free(trackable);
    }

    pthread_rwlock_destroy(&tracker->lock);
    close(tracker->handle);
    free(tracker);
  }
}

int FlushReliableTracker(struct ReliableTracker* tracker)
{
  struct uffdio_writeprotect protection;
  struct ReliableTrackable* trackable;
  struct ReliableTrackable* previous;
  struct ReliableTrackable* next;
  struct ReliableTrackable* list;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  ptrdiff_t delta;
  uint32_t count;
  uint64_t epoch;
  uint64_t mask;
  size_t index;
  size_t page;
  int result;

  if ((tracker != NULL) &&
      (atomic_fetch_and_explicit(&tracker->state, ~RELIABLE_TRACKER_STATE_KICK, memory_order_relaxed) & RELIABLE_TRACKER_STATE_KICK))
  {
    memset(&protection, 0, sizeof(struct uffdio_writeprotect));

    pool  = NULL;
    list  = NULL;
    count = 0;
    epoch = MakeEpoch(tracker);

    // Scan for changes

    pthread_rwlock_rdlock(&tracker->lock);

    for (trackable = tracker->list; trackable != NULL; trackable = trackable->next)
    {
      if (atomic_fetch_and_explicit(&trackable->state, ~RELIABLE_TRACKABLE_STATE_DURTY, memory_order_relaxed) & RELIABLE_TRACKABLE_STATE_DURTY)
      {
        for (index = 0; index < trackable->length; ++ index)
        {
          if (mask = atomic_exchange_explicit(trackable->map + index, 0ULL, memory_order_relaxed))
          {
            pool                    = trackable->pool;
            protection.mode         = UFFDIO_WRITEPROTECT_MODE_WP;
            protection.range.start  = trackable->range.start + tracker->size * index * 64;
            protection.range.len    = tracker->size << 6;
            delta                   = (protection.range.start + protection.range.len) - (trackable->range.start + trackable->range.len);
            protection.range.len   -= (delta > 0) ? delta : 0;

            if (CallIOCTL(tracker->handle, UFFDIO_WRITEPROTECT, &protection) < 0)
            {
              atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE | RELIABLE_TRACKER_STATE_KICK, memory_order_relaxed);
              atomic_fetch_or_explicit(trackable->map + index, mask, memory_order_relaxed);
            }

            while (mask != 0ULL)
            {
              page  = __builtin_ctzll(mask);
              mask &= mask - 1;
              page += index << 6;

              HandleDurtyPage(tracker, trackable, page, epoch);
            }
          }
        }
      }

      share  = trackable->share;
      count += (atomic_load_explicit(&share->weight, memory_order_relaxed) < RELIABLE_WEIGHT_STRONG);
    }

    pthread_rwlock_unlock(&tracker->lock);

    // Commit changes

    if (pool != NULL)
    {
      // Two shots in one: [1] at least one trackable has been changed, [2] the pool is being reused reused to make CallReliableMonitor()
      CallReliableMonitor(RELIABLE_MONITOR_FLUSH_COMMIT, pool, NULL, NULL);
    }

    // Cleanup unused trackables

    if (count != 0)
    {
      pthread_rwlock_wrlock(&tracker->lock);

      for (trackable = tracker->list; (trackable != NULL) && (count != 0); trackable = next)
      {
        next  = trackable->next;
        share = trackable->share;

        if (atomic_load_explicit(&share->weight, memory_order_relaxed) < RELIABLE_WEIGHT_STRONG)
        {
          if (~atomic_load_explicit(&trackable->state, memory_order_relaxed) & RELIABLE_TRACKABLE_STATE_DURTY)
          {
            previous           = trackable->previous;
            share->closures[0] = NULL;

            if (next     != NULL) next->previous = previous;
            if (previous != NULL) previous->next = next;
            else                  tracker->list  = next;

            trackable->next = list;
            list            = trackable;
          }

          count --;
        }
      }

      pthread_rwlock_unlock(&tracker->lock);

      for (trackable = list; trackable != NULL; trackable = next)
      {
        next   = trackable->next;
        result = CallIOCTL(tracker->handle, UFFDIO_UNREGISTER, &trackable->range);
        atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE * (result < 0), memory_order_relaxed);

        RetireReliableShare(trackable->share);
        free(trackable);
      }
    }

    return 0;
  }

  return -ENOENT;
}

int LockReliableShare(struct ReliableShare* share)
{
  struct uffdio_writeprotect protection;
  struct ReliableTrackable* trackable;
  struct ReliableTracker* tracker;
  int result;

  result = -1;

  if (trackable = (struct ReliableTrackable*)share->closures[0])
  {
    tracker = trackable->tracker;
    result  = atomic_fetch_or_explicit(&trackable->state, RELIABLE_TRACKABLE_STATE_LOCK, memory_order_relaxed) & RELIABLE_TRACKABLE_STATE_LOCK;

    if (result == 0)
    {
      memset(&protection, 0, sizeof(struct uffdio_writeprotect));

      protection.mode  = UFFDIO_WRITEPROTECT_MODE_WP;
      protection.range = trackable->range;
      result           = CallIOCTL(tracker->handle, UFFDIO_WRITEPROTECT, &protection);

      atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE * (result < 0), memory_order_relaxed);
    }
  }

  return result;
}

int UnlockReliableShare(struct ReliableShare* share)
{
  struct uffdio_writeprotect protection;
  struct ReliableTrackable* trackable;
  struct ReliableTracker* tracker;
  uint64_t mask;
  size_t index;
  size_t page;
  int result;

  result = -1;

  if ((trackable = (struct ReliableTrackable*)share->closures[0]) &&
      (atomic_fetch_and_explicit(&trackable->state, ~RELIABLE_TRACKABLE_STATE_LOCK, memory_order_relaxed) & RELIABLE_TRACKABLE_STATE_LOCK))
  {
    memset(&protection, 0, sizeof(struct uffdio_writeprotect));

    tracker              = trackable->tracker;
    result               = 0;
    protection.mode      = 0;
    protection.range.len = tracker->size;

    for (index = 0; index < trackable->length; ++ index)
    {
      if (mask = atomic_load_explicit(trackable->map + index, memory_order_relaxed))
      {
        while (mask != 0ULL)
        {
          page  = __builtin_ctzll(mask);
          mask &= mask - 1;
          page += index << 6;

          protection.mode        = 0;
          protection.range.start = trackable->range.start + tracker->size * page;
          protection.range.len   = tracker->size;

          result |= CallIOCTL(tracker->handle, UFFDIO_WRITEPROTECT, &protection);
        }
      }
    }

    atomic_fetch_or_explicit(&tracker->state, RELIABLE_TRACKER_STATE_FAILURE * (result < 0), memory_order_relaxed);
  }

  return result;
}
