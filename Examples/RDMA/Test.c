#define _GNU_SOURCE

#include <malloc.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>

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

int main(int count, char** arguments)
{
  struct sigaction action;

  int handle;
  struct ReliablePool* pool;
  struct ReliableTracker* tracker;
  struct ReliableIndexer* indexer;

  struct FastRing* ring;
  struct FastRingDescriptor* waiter;

  AvahiPoll* poll;

  action.sa_handler = HandleSignal;
  action.sa_flags   = SA_NODEFER | SA_RESTART;

  sigemptyset(&action.sa_mask);

  sigaction(SIGHUP,  &action, NULL);
  sigaction(SIGINT,  &action, NULL);
  sigaction(SIGTERM, &action, NULL);
  sigaction(SIGQUIT, &action, NULL);

  handle  = memfd_create("Test", MFD_CLOEXEC);
  indexer = CreateReliableIndexer(NULL);
  tracker = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
  pool    = CreateReliablePool(handle, "Test", 100, 0, &tracker->super, NULL, NULL);

  ring   = CreateFastRing(0);
  poll   = CreateFastAvahiPoll(ring);
  waiter = SubmitReliableWaiter(ring, tracker);

  printf("Started\n");

  while ((atomic_load_explicit(&state, memory_order_relaxed) == STATE_RUNNING) &&
         (WaitForFastRing(ring, 200, NULL) >= 0));

  printf("Stopped\n");

  CancelReliableWaiter(waiter);
  ReleaseFastAvahiPoll(poll);
  ReleaseFastRing(ring);

  ReleaseReliablePool(pool);
  ReleaseReliableTracker(tracker);
  ReleaseReliableIndexer(indexer);
  close(handle);

  return 0;
}