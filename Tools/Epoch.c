#define _GNU_SOURCE

#include "Epoch.h"

#include <unistd.h>
#include <sys/mman.h>

#include "Rescue.h"

#define NANOSECONDS  1000000000L

static int state                  = EPOCH_UNKNOWN;
static int handle                 = -1;
static struct EpochData current   = { 0 };
static struct timespec correction = { 0, 0 };

static void SubtractTime(struct timespec* value, const struct timespec* subtrahend)
{
  value->tv_sec  -= subtrahend->tv_sec;
  value->tv_nsec -= subtrahend->tv_nsec;

  if (value->tv_nsec < 0)
  {
    value->tv_sec  --;
    value->tv_nsec += NANOSECONDS;
  }
}

static void AddTime(struct timespec* value, const struct timespec* addend)
{
  value->tv_sec  += addend->tv_sec;
  value->tv_nsec += addend->tv_nsec;

  if (value->tv_nsec >= NANOSECONDS)
  {
    value->tv_sec  ++;
    value->tv_nsec -= NANOSECONDS;
  }
}

static void __attribute__((constructor(106))) Initialize()  // Should be initialized AFTER Rescue
{
  int result;
  struct timespec elapsed;
  struct EpochData previous;

  handle = GetRescuedHandle("Epoch");

  if (handle < 0)
  {
    handle = memfd_create("Epoch", MFD_CLOEXEC);
    AddRescuedHandle(handle, "Epoch");
  }

  if ((result = sd_id128_get_boot(&current.boot)) < 0)
  {
    // Correction stays zero, the record is not updated
    state = result;
    return;
  }

  // Pair is taken as close as possible, the gap between the calls is the error of the correction
  clock_gettime(CLOCK_MONOTONIC, &current.monotonic);
  clock_gettime(CLOCK_REALTIME,  &current.absolute);

  result = EPOCH_UNKNOWN;

  if ((handle >= 0) &&
      (pread(handle, &previous, sizeof(struct EpochData), 0) == sizeof(struct EpochData)))
  {
    result = EPOCH_SAME_BOOT;

    if (!sd_id128_equal(previous.boot, current.boot))
    {
      // Time passed between two records is measured by CLOCK_REALTIME, it can not be negative
      elapsed = current.absolute;
      SubtractTime(&elapsed, &previous.absolute);

      if (elapsed.tv_sec < 0)
      {
        elapsed.tv_sec  = 0;
        elapsed.tv_nsec = 0;
      }

      // Previous monotonic point moved to now in the old time base versus the same point in the new one
      correction = current.monotonic;
      SubtractTime(&correction, &previous.monotonic);
      SubtractTime(&correction, &elapsed);

      result = EPOCH_NEW_BOOT;
    }
  }

  // The record is stored before recovery: a crash inside recovery must not apply the correction twice
  pwrite(handle, &current, sizeof(struct EpochData), 0);

  state = result;
}

static void UpdateEpoch()
{
  if ((handle >= 0) &&
      (state  >= 0))
  {
    clock_gettime(CLOCK_MONOTONIC, &current.monotonic);
    clock_gettime(CLOCK_REALTIME,  &current.absolute);

    // Single small write, a process death can not tear it
    pwrite(handle, &current, sizeof(struct EpochData), 0);
  }
}

static void __attribute__((destructor)) Release()
{
  UpdateEpoch();

  if (handle >= 0)
  {
    // The copy stays in the fdstore
    close(handle);
    handle = -1;
  }
}

int GetEpochState()
{
  return state;
}

void GetEpochCorrection(struct timespec* value)
{
  *value = correction;
}

// Zero stands for "not set" in stored records and is kept as is,
// results before the start of the current boot are clamped to the smallest non-zero value

void FixEpochPreciseTime(struct timespec* value)
{
  if ((value->tv_sec  != 0) ||
      (value->tv_nsec != 0))
  {
    AddTime(value, &correction);

    if ((value->tv_sec < 0) ||
        ((value->tv_sec == 0) && (value->tv_nsec == 0)))
    {
      value->tv_sec  = 0;
      value->tv_nsec = 1;
    }
  }
}

void FixEpochCertainTime(struct timeval* value)
{
  struct timespec time;

  if ((value->tv_sec  != 0) ||
      (value->tv_usec != 0))
  {
    time.tv_sec  = value->tv_sec;
    time.tv_nsec = value->tv_usec * 1000L;

    FixEpochPreciseTime(&time);

    value->tv_sec  = time.tv_sec;
    value->tv_usec = time.tv_nsec / 1000L;
    value->tv_usec += (value->tv_sec == 0) && (value->tv_usec == 0);
  }
}

time_t FixEpochTime(time_t value)
{
  if (value != 0)
  {
    value += correction.tv_sec + (correction.tv_nsec >= (NANOSECONDS / 2));
    value += (value <= 0) * (1 - value);
  }

  return value;
}
