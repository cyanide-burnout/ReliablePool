#include "ReliableWaiter.h"

#include <linux/futex.h>

static int HandleWaiterCompletion(struct FastRingDescriptor* descriptor, struct io_uring_cqe* completion, int reason)
{
  struct ReliableTracker* tracker;

  if ((completion != NULL) &&
      (tracker = (struct ReliableTracker*)descriptor->closure))
  {
    FlushReliableTracker(tracker);

    descriptor->submission.off = atomic_load_explicit(&tracker->state, memory_order_relaxed) & ~RELIABLE_TRACKER_STATE_KICK;
    SubmitFastRingDescriptor(descriptor, 0);
    return 1;
  }

  return 0;
}

struct FastRingDescriptor* SubmitReliableWaiter(struct FastRing* ring, struct ReliableTracker* tracker)
{
  struct FastRingDescriptor* descriptor;
  uint32_t state;

  descriptor = NULL;

  if ((ring    != NULL) &&
      (tracker != NULL) &&
      (io_uring_opcode_supported(ring->probe, IORING_OP_FUTEX_WAIT) != 0))
  {
    state = atomic_load_explicit(&tracker->state, memory_order_relaxed) & ~RELIABLE_TRACKER_STATE_KICK;

    if ((state & RELIABLE_TRACKER_STATE_ACTIVE) &&
        (descriptor = AllocateFastRingDescriptor(ring, HandleWaiterCompletion, tracker)))
    {
      io_uring_prep_futex_wait(&descriptor->submission, (uint32_t*)&tracker->state, state, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
      SubmitFastRingDescriptor(descriptor, 0);
    }
  }

  return descriptor;
}

void CancelReliableWaiter(struct FastRingDescriptor* descriptor)
{
  if ((descriptor != NULL) &&
      (descriptor->submission.opcode == IORING_OP_FUTEX_WAIT))
  {
    descriptor->function = NULL;
    descriptor->closure  = NULL;

    atomic_fetch_add_explicit(&descriptor->references, 1, memory_order_relaxed);
    io_uring_initialize_sqe(&descriptor->submission);
    io_uring_prep_cancel64(&descriptor->submission, descriptor->identifier, 0);
    SubmitFastRingDescriptor(descriptor, RING_DESC_OPTION_IGNORE);
  }
}
