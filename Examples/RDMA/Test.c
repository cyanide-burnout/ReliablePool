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
  struct InstantReplicator* replicator;

  struct FastRing* ring;
  struct FastRingDescriptor* waiter;
  struct FastRingDescriptor* timeout;
  struct InstantDiscovery* discovery;

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

  handle     = memfd_create("Test", MFD_CLOEXEC);
  replicator = CreateInstantReplicator(0, "Test", "Secret", &monitor);
  indexer    = CreateReliableIndexer(&replicator->super);
  tracker    = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
  pool       = CreateReliablePool(handle, "Test", 50, 0, &tracker->super, NULL, NULL);

  ring      = CreateFastRing(0);
  poll      = CreateFastAvahiPoll(ring);
  waiter    = SubmitReliableWaiter(ring, tracker);
  discovery = CreateInstantDiscovery(poll, replicator);
  timeout   = SetFastRingTimeout(ring, NULL, 100, TIMEOUT_FLAG_REPEAT, HandleTimeoutCompletion, pool);

  if (~atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
  {
    printf(
      "It seems like the process has not enough capabilities to use ReliableTracker\n"
      "Please execute 'sudo setcap cap_sys_ptrace=ep %s' or run under root\n\n",
      arguments[0]);
    atomic_store_explicit(&state, 0, memory_order_relaxed);
  }

  if (replicator->listener == NULL)
  {
    printf("Failed to open RDMA port\n\n");
    atomic_store_explicit(&state, 0, memory_order_relaxed);
  }

  if (discovery == NULL)
  {
    printf("Failed to create avahi-client\n\n");
    atomic_store_explicit(&state, 0, memory_order_relaxed);
  }

  printf("Started\n");

  while ((atomic_load_explicit(&state, memory_order_relaxed) == STATE_RUNNING) &&
         (WaitForFastRing(ring, 200, NULL) >= 0));

  printf("Stopped\n");

  FlushReliableTracker(tracker);

  SetFastRingTimeout(ring, timeout, -1, 0, NULL, NULL);
  ReleaseInstantDiscovery(discovery);
  CancelReliableWaiter(waiter);
  ReleaseFastAvahiPoll(poll);
  ReleaseFastRing(ring);

  ReleaseReliablePool(pool);
  ReleaseReliableTracker(tracker);
  ReleaseReliableIndexer(indexer);
  ReleaseInstantReplicator(replicator);
  close(handle);

  return 0;
}