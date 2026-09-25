#include "Collapse.h"

#include <malloc.h>
#include <string.h>
#include <sys/stat.h>
#include <systemd/sd-bus.h>
#include <systemd/sd-login.h>

#define COLLAPSE_TIMEOUT  1000000ULL

static const char* const targets[] =
{
  "kexec.target",
  "soft-reboot.target",
  "reboot.target",
  "poweroff.target",
  "halt.target",
  NULL
};

int GetCollapseCause()
{
  sd_bus* bus;
  sd_bus_message* reply;
  const char* name;
  const char* type;
  char* unit;
  int result;
  int status;
  int index;

  // PID1 installs the whole shutdown transaction before it runs the first job,
  // so the target's start job is already queued when our stop job sends SIGTERM

  bus    = NULL;
  unit   = NULL;
  reply  = NULL;
  result = 0;

  if (((status = sd_pid_get_unit(0, &unit)) >= 0) &&
      ((status = sd_bus_open_system(&bus))  >= 0) &&
      ((status = sd_bus_set_method_call_timeout(bus, COLLAPSE_TIMEOUT)) >= 0) &&
      ((status = sd_bus_call_method(bus, "org.freedesktop.systemd1", "/org/freedesktop/systemd1", "org.freedesktop.systemd1.Manager", "ListJobs", NULL, &reply, NULL)) >= 0) &&
      ((status = sd_bus_message_enter_container(reply, SD_BUS_TYPE_ARRAY, "(usssoo)")) >= 0))
  {
    while ((status = sd_bus_message_read(reply, "(usssoo)", NULL, &name, &type, NULL, NULL, NULL)) > 0)
    {
      if (strcmp(type, "start") == 0)
        for (index = 0; targets[index] != NULL; index ++)
          result |= (strcmp(name, targets[index]) == 0) * (COLLAPSE_KEXEC << index);

      if ((strcmp(type, "restart") == 0) &&
          (strcmp(name, unit)      == 0))
        result |= COLLAPSE_RESTART;
    }
  }

  sd_bus_message_unref(reply);
  sd_bus_flush_close_unref(bus);
  free(unit);

  return (status < 0) ? status : result;
}

int IsLiveUpdateAvailable()
{
  struct stat status;

  return
    (stat("/dev/liveupdate", &status) == 0) &&
    (S_ISCHR(status.st_mode));
}

int CanSurvive()
{
  int cause;

  cause = GetCollapseCause();

  return (cause < 0) ? cause :
    ((cause & (COLLAPSE_RESTART | COLLAPSE_SOFT_REBOOT | COLLAPSE_KEXEC)) != 0) &&
    ((cause & COLLAPSE_SYSTEM & ~(COLLAPSE_SOFT_REBOOT | COLLAPSE_KEXEC)) == 0) &&
    (((cause & COLLAPSE_KEXEC) == 0) || IsLiveUpdateAvailable());
}
