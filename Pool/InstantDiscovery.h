#ifndef INSTANTDISCOVERY_H
#define INSTANTDISCOVERY_H

#include <avahi-client/client.h>
#include <avahi-client/lookup.h>
#include <avahi-client/publish.h>

#include "InstantReplicator.h"

#ifdef __cplusplus
extern "C"
{
#endif

struct InstantDiscovery
{
  AvahiPoll* poll;
  AvahiClient* client;
  AvahiTimeout* timeout;
  AvahiEntryGroup* group;
  AvahiServiceBrowser* browser;
  struct InstantReplicator* replicator;

  int error;      // Last error
  char* name;     // Local mDNS service name
  uint16_t port;  // Local RC port
};

struct InstantDiscovery* CreateInstantDiscovery(AvahiPoll* poll, struct InstantReplicator* replicator);
void ReleaseInstantDiscovery(struct InstantDiscovery* discovery);

#ifdef __cplusplus
}
#endif

#endif
