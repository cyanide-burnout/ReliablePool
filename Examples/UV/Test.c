#define _GNU_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/mman.h>
#include <sys/random.h>

#include <uv.h>

#include "ReliablePool.h"
#include "ReliableTracker.h"
#include "ReliableIndexer.h"
#include "InstantReplicator.h"

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  char buffer[64];

  switch (event)
  {
    case RELIABLE_MONITOR_SHARE_CHANGE:
      uv_async_send((uv_async_t*)closure);
      break;

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

    case RELIABLE_MONITOR_BLOCK_ARRIVAL:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) arrived: %s\n", block->number, buffer, block->data);
      break;

    case RELIABLE_MONITOR_BLOCK_REMOVAL:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) removed\n", block->number, buffer);
      break;

    case RELIABLE_MONITOR_BLOCK_DAMAGE:
      uuid_unparse_lower(block->identifier, buffer);
      printf("Block %u (%s) damaged\n", block->number, buffer);
      break;
  }
}

static void HandleReplicatorEvent(int event, struct InstantPeer* peer, const char* data, int parameter, void* closure)
{
  char buffer[64];

  switch (event)
  {
    case INSTANT_REPLICATOR_EVENT_FLUSH:
      uv_async_send((uv_async_t*)closure);
      break;

    case INSTANT_REPLICATOR_EVENT_CONNECTED:
      uuid_unparse_lower(peer->identifier, buffer);
      printf("Peer %s conected\n", buffer);
      break;

    case INSTANT_REPLICATOR_EVENT_DISCONNECTED:
      uuid_unparse_lower(peer->identifier, buffer);
      printf("Peer %s disconected\n", buffer);
      break;

    case INSTANT_REPLICATOR_EVENT_USER_MESSAGE:
      break;
  }
}

static void GenerateActivity(struct ReliablePool* pool)
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

static void HandleTimer(uv_timer_t* timer)
{
  GenerateActivity((struct ReliablePool*)timer->data);
}

static void HandleTrackerAsync(uv_async_t* async)
{
  FlushReliableTracker((struct ReliableTracker*)async->data);
}

static void HandleReplicatorAsync(uv_async_t* async)
{
  FlushInstantReplicator((struct InstantReplicator*)async->data);
}

static void HandleSignal(uv_signal_t* signal, int number)
{
  uv_stop(signal->loop);
}

int main(int count, char** arguments)
{
  uv_loop_t* loop;
  uv_timer_t timer;
  uv_async_t asyncs[2];
  uv_signal_t signals[4];

  int handle;
  struct ReliablePool* pool;
  struct ReliableMonitor monitor;
  struct ReliableTracker* tracker;
  struct ReliableIndexer* indexer;
  struct InstantReplicator* replicator;

  loop = uv_default_loop();

  uv_timer_init(loop, &timer);
  uv_signal_init(loop, signals + 0);
  uv_signal_init(loop, signals + 1);
  uv_signal_init(loop, signals + 2);
  uv_signal_init(loop, signals + 3);
  uv_async_init(loop, asyncs + 0, HandleTrackerAsync);
  uv_async_init(loop, asyncs + 1, HandleReplicatorAsync);

  memset(&monitor, 0, sizeof(struct ReliableMonitor));

  monitor.function = HandleMonitorEvent;
  monitor.closure  = asyncs + 0;

  handle     = memfd_create("Test", MFD_CLOEXEC);
  replicator = CreateInstantReplicator(0, NULL, "Test", "Secret", HandleReplicatorEvent, asyncs + 1, &monitor);
  indexer    = CreateReliableIndexer(&replicator->super);
  tracker    = CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super);
  pool       = CreateReliablePool(handle, "Test", 50, 0, &tracker->super, NULL, NULL);

  if (~atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
  {
    printf(
      "It seems like the process has not enough capabilities to use ReliableTracker\n"
      "Please execute 'sudo setcap cap_sys_ptrace=ep %s' or run under root\n\n",
      arguments[0]);
    goto Cleanup;
  }

  if (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
  {
    printf("Failed to open RDMA port\n\n");
    goto Cleanup;
  }

  printf("Started\n");

  timer.data     = pool;
  asyncs[0].data = tracker;
  asyncs[1].data = replicator;

  uv_signal_start(signals + 0, HandleSignal, SIGHUP);
  uv_signal_start(signals + 1, HandleSignal, SIGINT);
  uv_signal_start(signals + 2, HandleSignal, SIGTERM);
  uv_signal_start(signals + 3, HandleSignal, SIGQUIT);
  uv_timer_start(&timer, HandleTimer, 100, 100);

  uv_run(loop, UV_RUN_DEFAULT);

  FlushReliableTracker(tracker);
  FlushInstantReplicator(replicator);

  uv_timer_stop(&timer);
  uv_signal_stop(signals + 0);
  uv_signal_stop(signals + 1);
  uv_signal_stop(signals + 2);
  uv_signal_stop(signals + 3);

  printf("Stopped\n");

  Cleanup:

  ReleaseReliablePool(pool);
  ReleaseReliableTracker(tracker);
  ReleaseReliableIndexer(indexer);
  ReleaseInstantReplicator(replicator);
  close(handle);

  return 0;
}
