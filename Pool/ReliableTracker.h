#ifndef RELIABLETRACKER_H
#define RELIABLETRACKER_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "ReliablePool.h"

#include <linux/userfaultfd.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define RELIABLE_MONITOR_SHARE_CHANGE  10  // share - actual share waving modified page, called in monitoring thread
#define RELIABLE_MONITOR_BLOCK_CHANGE  11  // share - unprotected shadow copy of operational share, called by FlushReliableTracker()
#define RELIABLE_MONITOR_FLUSH_COMMIT  12

#define RELIABLE_TRACKER_STATE_ACTIVE   (1 << 0)
#define RELIABLE_TRACKER_STATE_FAILURE  (1 << 1)
#define RELIABLE_TRACKER_STATE_KICK     (1 << 2)

#define RELIABLE_TRACKER_FLAG_ID_HOST     (1 << 0)   // ┌
#define RELIABLE_TRACKER_FLAG_ID_PROCESS  (1 << 1)   // │ Host ID generation principles
#define RELIABLE_TRACKER_FLAG_ID_UNIQUE   (1 << 2)   // └
#define RELIABLE_TRACKER_FLAG_FORCE_MARK  (1 << 16)  // Mark blocks on every flush to reduce CRC checks, it makes negative influence on remote replication

#define RELIABLE_TRACKABLE_STATE_DURTY  (1 << 0)
#define RELIABLE_TRACKABLE_STATE_LOCK   (1 << 1)

struct ReliableTracker;
struct ReliableTrackedPool;
struct ReliableTrackedShare;

typedef ATOMIC(uint64_t) TrackablePageMap;

struct ReliableTrackable
{
  struct ReliableTrackable* previous;
  struct ReliableTrackable* next;

  struct ReliableTracker* tracker;
  struct ReliableShare* share;
  struct ReliablePool* pool;

  struct uffdio_range range;
  ATOMIC(uint32_t) state;
  size_t length;

  TrackablePageMap map[0];
};

struct ReliableTracker
{
  struct ReliableMonitor super;

  ATOMIC(uint32_t) state;  //
  ATOMIC(uint32_t) epoch;  // Epoch counter
  ATOMIC(uint64_t) last;   // Last used mark value
  pthread_rwlock_t lock;   //
  pthread_t thread;        //
  uint32_t node;           // Node identifier
  size_t size;             // Page size
  int handle;              // userfaultfd

  struct ReliableTrackable* list;
};

// FlushReliableTracker() should be called in idempotent state (in main loop or under global lock)

struct ReliableTracker* CreateReliableTracker(uint32_t flags, struct ReliableMonitor* next);
void ReleaseReliableTracker(struct ReliableTracker* tracker);
int FlushReliableTracker(struct ReliableTracker* tracker);

// Following functions are reserved for hot replication with zero-copy capabilities to lock data modifications

int LockReliableShare(struct ReliableShare* share);
int UnlockReliableShare(struct ReliableShare* share);

int64_t GetReliableTrackerClockVector(struct timespec* remote);

#ifdef __cplusplus
}
#endif

#endif
