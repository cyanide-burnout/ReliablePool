#include "InstantWaiter.h"

#include <linux/futex.h>

static int HandleWaiterCompletion(struct FastRingDescriptor* descriptor, struct io_uring_cqe* completion, int reason)
{
  struct InstantReplicator* replicator;

  if ((completion != NULL) &&
      (replicator  = (struct InstantReplicator*)descriptor->closure) &&
      (FlushInstantReplicator(replicator) == 0))
  {
    SubmitFastRingDescriptor(descriptor, 0);
    return 1;
  }

  return 0;
}

struct FastRingDescriptor* SubmitInstantWaiter(struct FastRing* ring, struct InstantReplicator* replicator)
{
  struct FastRingDescriptor* descriptor;

  descriptor = NULL;

  if ((ring       != NULL) &&
      (replicator != NULL) &&
      (io_uring_opcode_supported(ring->probe, IORING_OP_FUTEX_WAIT) != 0) &&
      (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE) &&
      (descriptor = AllocateFastRingDescriptor(ring, HandleWaiterCompletion, replicator)))
  {
    io_uring_prep_futex_wait(&descriptor->submission, (uint32_t*)&replicator->state, INSTANT_REPLICATOR_STATE_ACTIVE, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
    SubmitFastRingDescriptor(descriptor, 0);
  }

  return descriptor;
}

void CancelInstantWaiter(struct FastRingDescriptor* descriptor)
{
  if ((descriptor != NULL) &&
      (descriptor->function == HandleWaiterCompletion) &&
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
