#include "InstantDiscovery.h"

#include <malloc.h>
#include <netinet/in.h>
#include <avahi-common/error.h>
#include <avahi-common/malloc.h>
#include <avahi-common/alternative.h>

#define REPLICATOR_MDNS_SERVICE  "_replicator._tcp"  //
#define RESTART_TIMEOUT          200                 // Milliseconds

static void RegisterLocalService(struct InstantDiscovery* discovery);
static void RestartBrokenClient(struct InstantDiscovery* discovery);

static void HandleTimeoutEvent(AvahiTimeout* timeout, void* closure)
{
  RestartBrokenClient((struct InstantDiscovery*)closure);
}

static void HandleClientError(struct InstantDiscovery* discovery)
{
  struct timeval interval;
  AvahiPoll* poll;

  if (discovery->client != NULL)
  {
    // avahi_client_new() updates discovery->error directly
    discovery->error = avahi_client_errno(discovery->client);
  }

  if ((discovery->error == AVAHI_ERR_DISCONNECTED) ||
      (discovery->error == AVAHI_ERR_DBUS_ERROR)   ||
      (discovery->error == AVAHI_ERR_NO_DAEMON)    ||
      (discovery->error == AVAHI_ERR_TIMEOUT))
  {
    poll             = discovery->poll;
    interval.tv_sec  =  RESTART_TIMEOUT / 1000;
    interval.tv_usec = (RESTART_TIMEOUT % 1000) * 1000;

    if (discovery->timeout != NULL)
    {
      poll->timeout_update(discovery->timeout, &interval);
      return;
    }

    discovery->timeout = poll->timeout_new(discovery->poll, &interval, HandleTimeoutEvent, discovery);
  }
}

static void BuildLocalServiceAlternativeName(struct InstantDiscovery* discovery)
{
  char* name;

  name = avahi_alternative_service_name(discovery->name);
  avahi_free(discovery->name);
  discovery->name = name;

  RegisterLocalService(discovery);
}

static void HandleGroupEvent(AvahiEntryGroup* group, AvahiEntryGroupState state, void* closure)
{
  struct InstantDiscovery* discovery;

  discovery        = (struct InstantDiscovery*)closure;
  discovery->group = group;

  switch (state)
  {
    case AVAHI_ENTRY_GROUP_COLLISION:
      BuildLocalServiceAlternativeName(discovery);
      break;

    case AVAHI_ENTRY_GROUP_FAILURE:
      HandleClientError(discovery);
      break;
  }
}

static void RegisterLocalService(struct InstantDiscovery* discovery)
{
  int result;
  char buffer[128];
  struct InstantReplicator* replicator;

  if ((discovery->group == NULL) &&
      ((discovery->group = avahi_entry_group_new(discovery->client, HandleGroupEvent, discovery)) == NULL))
  {
    HandleClientError(discovery);
    return;
  }

  if (avahi_entry_group_is_empty(discovery->group))
  {
    replicator = discovery->replicator;
    result     = snprintf(buffer, sizeof(buffer), "instance=");
    uuid_unparse_lower(replicator->identifier, buffer + result);

    if (((result = avahi_entry_group_add_service(discovery->group, AVAHI_IF_UNSPEC, AVAHI_PROTO_UNSPEC, 0, discovery->name, REPLICATOR_MDNS_SERVICE, NULL, NULL, discovery->port, buffer, NULL)) < 0) ||
        ((result = avahi_entry_group_commit(discovery->group)) < 0))
    {
      if (result == AVAHI_ERR_COLLISION)
      {
        BuildLocalServiceAlternativeName(discovery);
        return;
      }

      HandleClientError(discovery);
    }
  }
}

static void BuildSocketAddress(struct sockaddr_storage* storage, const AvahiAddress* address, uint16_t port)
{
  struct sockaddr_in* v4;
  struct sockaddr_in6* v6;

  memset(storage, 0, sizeof(struct sockaddr_storage));

  switch (address->proto)
  {
    case AVAHI_PROTO_INET:
      v4                  = (struct sockaddr_in*)storage;
      v4->sin_family      = AF_INET;
      v4->sin_port        = htons(port);
      v4->sin_addr.s_addr = address->data.ipv4.address;
      break;

    case AVAHI_PROTO_INET6:
      v6              = (struct sockaddr_in6*)storage;
      v6->sin6_family = AF_INET6;
      v6->sin6_port   = htons(port);
      memcpy(v6->sin6_addr.s6_addr, address->data.ipv6.address, sizeof(AvahiIPv6Address));
      break;
  }
}

static void HandleResolverEvent(AvahiServiceResolver* resolver, AvahiIfIndex interface, AvahiProtocol protocol, AvahiResolverEvent event,
  const char* name, const char* type, const char* domain, const char* host, const AvahiAddress* address, uint16_t port,
  AvahiStringList* text, AvahiLookupResultFlags flags, void* closure)
{
  struct InstantDiscovery* discovery;
  struct sockaddr_storage storage;
  AvahiStringList* item;
  uuid_t identifier;
  size_t length;
  char* value;
  char* key;

  discovery = (struct InstantDiscovery*)closure;
  value     = NULL;
  key       = NULL;

  switch (event)
  {
    case AVAHI_RESOLVER_FAILURE:
      HandleClientError(discovery);
      break;

    case AVAHI_RESOLVER_FOUND:
      if (!((flags & AVAHI_LOOKUP_RESULT_LOCAL)   && (port == discovery->port)                   ||
            (address->proto == AVAHI_PROTO_INET)  && (address->data.data[0] == IN_LOOPBACKNET)   ||
            (address->proto == AVAHI_PROTO_INET6) && (IN6_IS_ADDR_LOOPBACK(address->data.data))) &&
          (item   = avahi_string_list_find(text, "instance")) && (avahi_string_list_get_pair(item, &key, &value, &length) == 0) &&
          (value != NULL)                                     && (uuid_parse(value, identifier)                           == 0))
      {
        BuildSocketAddress(&storage, address, port);
        RegisterRemoteInstantReplicator(discovery->replicator, identifier, (struct sockaddr*)&storage);
      }
  }

  avahi_free(key);
  avahi_free(value);
  avahi_service_resolver_free(resolver);
}

static void HandleBrowserEvent(AvahiServiceBrowser* browser, AvahiIfIndex interface, AvahiProtocol protocol, AvahiBrowserEvent event,
  const char* name, const char* type, const char* domain, AvahiLookupResultFlags flags, void* closure)
{
  struct InstantReplicator* replicator;
  struct InstantDiscovery* discovery;
  int length;

  discovery = (struct InstantDiscovery*)closure;

  switch (event)
  {
    case AVAHI_BROWSER_FAILURE:
      HandleClientError(discovery);
      break;

    case AVAHI_BROWSER_NEW:
      replicator = discovery->replicator;
      length     = strlen(replicator->name);

      if ((strncmp(name, replicator->name, length) == 0) &&
          ((name[length] == '\0') || (name[length] == ' ')))
      {
        avahi_service_resolver_new(discovery->client, interface, protocol, name, type, domain, AVAHI_PROTO_UNSPEC, 0, HandleResolverEvent, discovery);
        break;
      }
  }
}

static void HandleClientEvent(AvahiClient* client, AvahiClientState state, void* closure)
{
  struct InstantDiscovery* discovery;

  discovery         = (struct InstantDiscovery*)closure;
  discovery->client = client;

  switch (state)
  {
    case AVAHI_CLIENT_S_RUNNING:
      RegisterLocalService(discovery);
      break;

    case AVAHI_CLIENT_FAILURE:
      HandleClientError(discovery);
      break;
  }
}

static void RestartBrokenClient(struct InstantDiscovery* discovery)
{
  if (discovery->browser != NULL)  avahi_service_browser_free(discovery->browser);
  if (discovery->group   != NULL)  avahi_entry_group_free(discovery->group);
  if (discovery->client  != NULL)  avahi_client_free(discovery->client);

  discovery->browser = NULL;
  discovery->group   = NULL;
  discovery->client  = NULL;

  if (((discovery->client  = avahi_client_new(discovery->poll, 0, HandleClientEvent, discovery, &discovery->error)) == NULL) ||
      ((discovery->browser = avahi_service_browser_new(discovery->client, AVAHI_IF_UNSPEC, AVAHI_PROTO_UNSPEC, REPLICATOR_MDNS_SERVICE, NULL, 0, HandleBrowserEvent, discovery)) == NULL))
  {
    HandleClientError(discovery);
    return;
  }
}

struct InstantDiscovery* CreateInstantDiscovery(AvahiPoll* poll, struct InstantReplicator* replicator)
{
  struct InstantDiscovery* discovery;

  discovery = NULL;

  if ((poll                 != NULL) &&
      (replicator           != NULL) &&
      (replicator->listener != NULL) &&
      (discovery = (struct InstantDiscovery*)calloc(1, sizeof(struct InstantDiscovery))))
  {
    discovery->replicator = replicator;
    discovery->poll       = poll;
    discovery->name       = avahi_strdup(replicator->name);
    discovery->port       = ntohs(rdma_get_src_port(replicator->listener));

    if (((discovery->client  = avahi_client_new(discovery->poll, 0, HandleClientEvent, discovery, &discovery->error)) == NULL) ||
        ((discovery->browser = avahi_service_browser_new(discovery->client, AVAHI_IF_UNSPEC, AVAHI_PROTO_UNSPEC, REPLICATOR_MDNS_SERVICE, NULL, 0, HandleBrowserEvent, discovery)) == NULL))
    {
      ReleaseInstantDiscovery(discovery);
      return NULL;
    }
  }

  return discovery;
}

void ReleaseInstantDiscovery(struct InstantDiscovery* discovery)
{
  if (discovery != NULL)
  {
    if (discovery->timeout != NULL)  discovery->poll->timeout_free(discovery->timeout);
    if (discovery->browser != NULL)  avahi_service_browser_free(discovery->browser);
    if (discovery->group   != NULL)  avahi_entry_group_free(discovery->group);
    if (discovery->client  != NULL)  avahi_client_free(discovery->client);
    avahi_free(discovery->name);
    free(discovery);
  }
}
