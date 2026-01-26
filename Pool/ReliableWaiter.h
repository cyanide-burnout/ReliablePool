#ifndef RELIABLEWAITER_H
#define RELIABLEWAITER_H

#include "FastRing.h"
#include "ReliableTracker.h"

#ifdef __cplusplus
extern "C"
{
#endif

struct FastRingDescriptor* SubmitReliableWaiter(struct FastRing* ring, struct ReliableTracker* tracker);
void CancelReliableWaiter(struct FastRingDescriptor* descriptor);

#ifdef __cplusplus
}
#endif

#endif
