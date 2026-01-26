#define _GNU_SOURCE

#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/random.h>

#include "FastRing.h"
#include "FastAvahiPoll.h"

#include "ReliablePool.h"
#include "ReliableTracker.h"
#include "ReliableIndexer.h"
#include "ReliableWaiter.h"

#include "InstantReplicator.h"
#include "InstantDiscovery.h"

#define STATE_RUNNING  -1

atomic_int state = { STATE_RUNNING };

static void HandleSignal(int signal)
{
  // Interrupt main loop in case of interruption signal
  atomic_store_explicit(&state, 0, memory_order_relaxed);
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  char buffer[64];

  switch (event)
  {
     case RELIABLE_MONITOR_BLOCK_ALLOCATE:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) allocated\n", block->number, buffer);
      break;

     case RELIABLE_MONITOR_BLOCK_RELEASE:
     case RELIABLE_MONITOR_BLOCK_FREE:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) released\n", block->number, buffer);
      break;

    case RELIABLE_MONITOR_BLOCK_CHANGE:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) changed: %s\n", block->number, buffer, block->data);
      break;
  }
}

static void GeenetateActivity(struct ReliablePool* pool)
{
  static struct ReliableDescriptor descriptors[4096] = { };

  struct ReliableDescriptor* descriptor;
  struct ReliableBlock* block;
  uint32_t number;

  getrandom((uint8_t*)&number, sizeof(uint32_t), 0);

  descriptor = descriptors + (number % 4096);

  if ((number & 0x00ff0000) &&
      (descriptor->block != NULL))
  {
    ReleaseReliableBlock(descriptor, RELIABLE_TYPE_FREE);
    return;
  }

  if (descriptor->block == NULL)
  {
    //
    AllocateReliableBlock(descriptor, pool, RELIABLE_TYPE_RECOVERABLE);
  }

  block = descriptor->block;

  sprintf((char*)block->data, "pid %d rng %08x", getpid(), number);
}

static void HandleTimeoutCompletion(struct FastRingDescriptor* descriptor)
{
  GeenetateActivity((struct ReliablePool*)descriptor->closure);
}

int main(int count, char** arguments)
{
  struct sigaction action;

  int handle;
  struct ReliablePool* pool;
  struct ReliableMonitor monitor;
  struct ReliableTracker* tracker;
  struct ReliableIndexer* indexer;

  struct FastRing* ring;
  struct FastRingDescriptor* waiter;
  struct FastRingDescriptor* timeout;

  AvahiPoll* poll;

  action.sa_handler = HandleSignal;
  action.sa_flags   = SA_NODEFER | SA_RESTART;

  sigemptyset(&action.sa_mask);
  sigaction(SIGHUP,  &action, NULL);
  sigaction(SIGINT,  &action, NULL);
  sigaction(SIGTERM, &action, NULL);
  sigaction(SIGQUIT, &action, NULL);

  memset(&monitor, 0, sizeof(struct ReliableMonitor));

  monitor.function = HandleMonitorEvent;

  handle  = memfd_create("Test", MFD_CLOEXEC);
  indexer = CreateReliableIndexer(&monitor);
  tracker = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
  pool    = CreateReliablePool(handle, "Test", 50, 0, &tracker->super, NULL, NULL);

  ring    = CreateFastRing(0);
  poll    = CreateFastAvahiPoll(ring);
  waiter  = SubmitReliableWaiter(ring, tracker);
  timeout = SetFastRingTimeout(ring, NULL, 100, TIMEOUT_FLAG_REPEAT, HandleTimeoutCompletion, pool);

  printf("Started\n");

  while ((atomic_load_explicit(&state, memory_order_relaxed) == STATE_RUNNING) &&
         (WaitForFastRing(ring, 200, NULL) >= 0));

  printf("Stopped\n");

  SetFastRingTimeout(ring, timeout, -1, 0, NULL, NULL);
  CancelReliableWaiter(waiter);
  ReleaseFastAvahiPoll(poll);
  ReleaseFastRing(ring);

  ReleaseReliablePool(pool);
  ReleaseReliableTracker(tracker);
  ReleaseReliableIndexer(indexer);
  close(handle);

  return 0;
}