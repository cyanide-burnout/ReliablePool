#ifndef INSTANTREPLICATOR_H
#define INSTANTREPLICATOR_H

#include "ReliablePool.h"

#include <uuid.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C"
{
#endif

struct InstantReplicator
{
  struct ReliableMonitor super;

  struct rdma_event_channel* channel;
  struct rdma_cm_id* listener;
  struct ibv_context* context;
  struct ibv_pd* protection;
  pthread_mutex_t lock;

  char* name;
  char* secret;
  uuid_t identifier;

};

struct InstantReplicator* CreateInstantReplicator(int port, const char* name, const char* secret, struct ReliableMonitor* next);
void ReleaseInstantReplicator(struct InstantReplicator* replicator);

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address);

#ifdef __cplusplus
}
#endif

#endif
