#ifndef COLLAPSE_H
#define COLLAPSE_H

#ifdef __cplusplus
extern "C"
{
#endif

#define COLLAPSE_RESTART      (1 << 0)

#define COLLAPSE_KEXEC        (1 << 1)
#define COLLAPSE_SOFT_REBOOT  (1 << 2)
#define COLLAPSE_REBOOT       (1 << 3)
#define COLLAPSE_POWEROFF     (1 << 4)
#define COLLAPSE_HALT         (1 << 5)

#define COLLAPSE_SYSTEM       (COLLAPSE_KEXEC | COLLAPSE_SOFT_REBOOT | COLLAPSE_REBOOT | COLLAPSE_POWEROFF | COLLAPSE_HALT)

// Returns bit field or negative errno on failure,
// special targets do not conflict with each other, so direct requests to PID1 may queue several of them

int GetCollapseCause();

// Returns non-zero when LUO is active in the running kernel (CONFIG_LIVEUPDATE, liveupdate=on and KHO),
// the device is only checked for presence: it is root-only and single-open, systemd-shutdown needs it on kexec

int IsLiveUpdateAvailable();

// Returns 1 when the service state survives the collapse (unit restart, soft-reboot, kexec with LUO),
// 0 when it does not, or negative errno on failure

int CanSurvive();

#ifdef __cplusplus
}
#endif

#endif
