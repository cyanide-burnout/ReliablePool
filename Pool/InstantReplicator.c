#include "InstantReplicator.h"

#include <malloc.h>
#include <string.h>

#include "ReliableTracker.h"
#include "ReliableIndexer.h"

static void AddMemoryRegion(struct InstantReplicator* replicator, struct ReliableShare* share)
{
  pthread_mutex_lock(&replicator->lock);

  share->closures[1] = ibv_reg_mr(replicator->protection, share->memory, share->size,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
 
  pthread_mutex_unlock(&replicator->lock);
}

static void RemoveMemoryRegion(struct InstantReplicator* replicator, struct ReliableShare* share)
{
  struct ibv_mr* region;

  if (region = (struct ibv_mr*)share->closures[1])
  {
    // ibv_dereg_mr() has no NULL-tolerance
    ibv_dereg_mr(region);
  }
}

static void HandleReliablePoolEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct InstantReplicator* replicator;

  replicator = (struct InstantReplicator*)closure;

  if (replicator->protection == NULL)
  {
    //
    return;
  }

  switch (event)
  {
    case RELIABLE_MONITOR_POOL_CREATE:
    case RELIABLE_MONITOR_SHARE_CREATE:
      AddMemoryRegion(replicator, share);
      break;

    case RELIABLE_MONITOR_POOL_RELEASE:
    case RELIABLE_MONITOR_SHARE_DESTROY:
      RemoveMemoryRegion(replicator, share);
      break;
  }
}

struct InstantReplicator* CreateInstantReplicator(int port, const char* name, const char* secret, struct ReliableMonitor* next)
{
  struct InstantReplicator* replicator;
  struct rdma_addrinfo* information;
  struct rdma_addrinfo hint;
  char service[16];

  if (replicator = (struct InstantReplicator*)calloc(1, sizeof(struct InstantReplicator)))
  {
    replicator->super.next     = next;
    replicator->super.closure  = replicator;
    replicator->super.function = HandleReliablePoolEvent;
    replicator->channel        = rdma_create_event_channel();
    replicator->secret         = strdup(secret);
    replicator->name           = strdup(name);

    uuid_generate(replicator->identifier);
    pthread_mutex_init(&replicator->lock, NULL);

    snprintf(service, sizeof(service), "%d", port);
    memset(&hint, 0, sizeof(struct rdma_addrinfo));

    hint.ai_flags      = RAI_PASSIVE;
    hint.ai_port_space = RDMA_PS_TCP;
    hint.ai_qp_type    = IBV_QPT_RC;
    information        = NULL;

    if ((replicator->channel  != NULL) &&
        ((rdma_getaddrinfo(NULL, service, &hint, &information)           < 0)  ||
         (rdma_create_ep(&replicator->listener, information, NULL, NULL) < 0)  ||
         (rdma_listen(replicator->listener, 16)                          < 0)  ||
         (rdma_migrate_id(replicator->listener, replicator->channel)     < 0)) &&
        (replicator->listener != NULL))
    {
      rdma_destroy_ep(replicator->listener);
      replicator->listener = NULL;
    }

    rdma_freeaddrinfo(information);

    if (replicator->listener != NULL)
    {
      replicator->context    = replicator->listener->verbs;
      replicator->protection = ibv_alloc_pd(replicator->context);
    }

  }

  return replicator;
}

void ReleaseInstantReplicator(struct InstantReplicator* replicator)
{
  if (replicator != NULL)
  {
    if (replicator->protection != NULL)  ibv_dealloc_pd(replicator->protection);
    if (replicator->listener   != NULL)  rdma_destroy_ep(replicator->listener);
    if (replicator->channel    != NULL)  rdma_destroy_event_channel(replicator->channel);
    pthread_mutex_destroy(&replicator->lock);
    free(replicator->secret);
    free(replicator->name);
    free(replicator);
  }
}

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address)
{
  return -1;
}
