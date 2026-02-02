#ifndef INSTANTWAITER_H
#define INSTANTWAITER_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "FastRing.h"
#include "InstantReplicator.h"

#ifdef __cplusplus
extern "C"
{
#endif

struct FastRingDescriptor* SubmitInstantWaiter(struct FastRing* ring, struct InstantReplicator* replicator);
void CancelInstantWaiter(struct FastRingDescriptor* descriptor);

#ifdef __cplusplus
}
#endif

#endif
