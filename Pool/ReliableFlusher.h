#ifndef RELIABLEFLUSHER_H
#define RELIABLEFLUSHER_H

#include "ReliableTracker.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define RELIABLE_FLUSHER_STATE_FAILURE  (1 << 0)

struct ReliableFlusher
{
  struct ReliableMonitor super;
  ATOMIC(uint32_t) state;
};

struct ReliableFlusher* CreateReliableFlusher(struct ReliableMonitor* next);
void ReleaseReliableFlusher(struct ReliableFlusher* flusher);

#ifdef __cplusplus
}
#endif

#endif
