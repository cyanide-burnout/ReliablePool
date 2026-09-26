#ifndef EPOCH_H
#define EPOCH_H

#include <time.h>
#include <sys/time.h>
#include <systemd/sd-id128.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define EPOCH_SAME_BOOT  0
#define EPOCH_NEW_BOOT   1
#define EPOCH_UNKNOWN    2

struct EpochData
{
  sd_id128_t boot;            // Kernel boot_id, changes on reboot and kexec, kept on soft-reboot
  struct timespec monotonic;  // CLOCK_MONOTONIC
  struct timespec absolute;   // CLOCK_REALTIME, taken right after monotonic
};

// On entry (constructor, after Rescue) the record of the previous instance is read, the correction is computed
// and the current record is stored immediately (memfd "Epoch" is kept by Rescue), on exit (destructor) it is stored again

// Returns EPOCH_* or negative errno on failure (correction is zero then)

int GetEpochState();

// Correction to add to CLOCK_MONOTONIC values stored by the previous instance, zero within the same boot

void GetEpochCorrection(struct timespec* value);

void FixEpochPreciseTime(struct timespec* value);
void FixEpochCertainTime(struct timeval* value);
time_t FixEpochTime(time_t value);

#ifdef __cplusplus
}
#endif

#endif
