#include "ReliableFlusher.h"

#include <malloc.h>
#include <sys/mman.h>

static void FlushReliableShare(struct ReliableFlusher* flusher, struct ReliableShare* share)
{
  if ((share != NULL) &&
      (msync(share->memory, share->size, MS_SYNC) < 0))
  {
    // Keep the failure flag sticky once any synchronization attempt fails
    atomic_fetch_or_explicit(&flusher->state, RELIABLE_FLUSHER_STATE_FAILURE, memory_order_relaxed);
  }
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  static __thread struct ReliableShare* last = NULL;

  switch (event)
  {
    case RELIABLE_MONITOR_FLUSH_COMMIT:
      FlushReliableShare((struct ReliableFlusher*)closure, last);
      last = NULL;
      break;

    case RELIABLE_MONITOR_BLOCK_CHANGE:
      if (last != share)
      {
        FlushReliableShare((struct ReliableFlusher*)closure, last);
        last = share;
      }
  }
}

struct ReliableFlusher* CreateReliableFlusher(struct ReliableMonitor* next)
{
  struct ReliableFlusher* flusher;

  if (flusher = (struct ReliableFlusher*)calloc(1, sizeof(struct ReliableFlusher)))
  {
    flusher->super.next     = next;
    flusher->super.closure  = flusher;
    flusher->super.function = HandleMonitorEvent;
  }

  return flusher;
}

void ReleaseReliableFlusher(struct ReliableFlusher* flusher)
{
  free(flusher);
}
