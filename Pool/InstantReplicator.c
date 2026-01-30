#include "InstantReplicator.h"

#include <poll.h>
#include <time.h>
#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <sys/random.h>
#include <sys/eventfd.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/crypto.h>

#include "ReliableTracker.h"
#include "ReliableIndexer.h"

#define POLL_TIMEOUT   200   // Milliseconds
#define QUEUE_LENGTH   4096  //
#define ATTEMPT_COUNT  128   //

/*
static void AddMemoryRegion(struct InstantReplicator* replicator, struct ReliableShare* share)
{
  pthread_mutex_lock(&replicator->lock);

  share->closures[1] = ibv_reg_mr(replicator->domain, share->memory, share->size,
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
*/

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct InstantReplicator* replicator;

  replicator = (struct InstantReplicator*)closure;

}

static void HandleApplicationEvent(struct InstantReplicator* replicator)
{

}

//

static void HandleCompletionChannel(struct InstantReplicator* replicator)
{

}

// Connection tracking

static void DestroyDescriptor(struct rdma_cm_id* descriptor, int condition)
{
  if ((condition  != 0) &&
      (descriptor != NULL))
  {
    if (descriptor->qp != NULL)
    {
      // Don't call rdma_destroy_ep() to prevent CQ destruction
      rdma_destroy_qp(descriptor);
    }

    rdma_destroy_id(descriptor);
  }
}

static int HandleConnectRequest(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor, struct rdma_conn_param* parameter)
{
  struct InstantPeer* peer;
  struct InstantHandshakeData* handshake;
  uint8_t digest[SHA256_DIGEST_LENGTH];

  peer      = NULL;
  handshake = NULL;

  if ((parameter != NULL) &&
      (parameter->private_data_len == sizeof(struct InstantHandshakeData)) &&
      (handshake = (struct InstantHandshakeData*)parameter->private_data)  &&
      (handshake->magic == INSTANT_MAGIC) &&
      (memcmp(handshake->name, replicator->handshake.name, INSTANT_SERVICE_NAME_LENGTH) == 0) &&
      (HMAC(EVP_sha256(), replicator->secret, strlen(replicator->secret), (uint8_t*)handshake, offsetof(struct InstantHandshakeData, digest), digest, NULL) != NULL) &&
      (memcmp(handshake->digest, digest, SHA256_DIGEST_LENGTH) == 0))
  {
    pthread_mutex_lock(&replicator->lock);
    for (peer = replicator->peers; (peer != NULL) && (uuid_compare(handshake->identifier, peer->identifier) != 0); peer = peer->next);
    pthread_mutex_unlock(&replicator->lock);
  }

  if ((peer == NULL) ||
      (peer->state != INSTANT_PEER_STATE_DISCONNECTED) ||
      (rdma_create_qp(descriptor, replicator->domain, &replicator->attribute) != 0) ||
      (rdma_accept(descriptor, &replicator->parameter) != 0))
  {
    rdma_reject(descriptor, NULL, 0);
    return -1;
  }

  peer->state         = INSTANT_PEER_STATE_CONNECTING;
  peer->descriptor    = descriptor;
  descriptor->context = peer;
  return 0;
}

static int HandleEstablished(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor)
{
  struct InstantPeer* peer;

  peer = (struct InstantPeer*)descriptor->context;

  peer->state      = INSTANT_PEER_STATE_CONNECTED;
  peer->descriptor = descriptor;
  peer->fails      = 0;
  peer->points[peer->round].rank = 0;

  return 0;
}

static int HandleDisconnected(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor, int reason)
{
  struct InstantPeer* peer;

  if ((peer = (struct InstantPeer*)descriptor->context) &&
      (peer->descriptor == descriptor))
  {
    peer->state      = INSTANT_PEER_STATE_DISCONNECTED;
    peer->descriptor = NULL;

    peer->points[peer->round].rank ++;
    peer->fails ++;
    peer->round ++;
    peer->round %= INSTANT_POINT_COUNT;
  }

  return -1;
}

static int HandleAddressResolved(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor)
{
  if (rdma_resolve_route(descriptor, POLL_TIMEOUT) != 0)
  {
    // Cleanup connection state
    return HandleDisconnected(replicator, descriptor, RDMA_CM_EVENT_ROUTE_ERROR);
  }

  return 0;
}

static int HandleRouteResolved(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor)
{
  if ((rdma_create_qp(descriptor, replicator->domain, &replicator->attribute) != 0) ||
      (rdma_connect(descriptor, &replicator->parameter)                       != 0))
  {
    // Cleanup connection state
    return HandleDisconnected(replicator, descriptor, RDMA_CM_EVENT_CONNECT_ERROR);    
  }

  return 0;
}

static void HandleEventChannel(struct InstantReplicator* replicator)
{
  struct rdma_cm_id* descriptor;
  struct rdma_cm_event* event;
  int result;

  while (rdma_get_cm_event(replicator->channel1, &event) == 0)
  {
    result     = 0;
    descriptor = event->id;

    switch (event->event)
    {
      case RDMA_CM_EVENT_CONNECT_REQUEST:  result = HandleConnectRequest(replicator, descriptor, &event->param.conn);  break;
      case RDMA_CM_EVENT_ADDR_RESOLVED:    result = HandleAddressResolved(replicator, descriptor);                     break;
      case RDMA_CM_EVENT_ROUTE_RESOLVED:   result = HandleRouteResolved(replicator, descriptor);                       break;
      case RDMA_CM_EVENT_ESTABLISHED:      result = HandleEstablished(replicator, descriptor);                         break;
      case RDMA_CM_EVENT_ADDR_ERROR:
      case RDMA_CM_EVENT_ROUTE_ERROR:
      case RDMA_CM_EVENT_CONNECT_ERROR:
      case RDMA_CM_EVENT_UNREACHABLE:
      case RDMA_CM_EVENT_REJECTED:
      case RDMA_CM_EVENT_DISCONNECTED:     result = HandleDisconnected(replicator, descriptor, event->event);          break;
    }

    rdma_ack_cm_event(event);
    DestroyDescriptor(descriptor, result);
  }
}

static int MakeConnectionAttempt(struct InstantReplicator* replicator, struct InstantPeer* peer)
{
  uint32_t number;
  struct InstantPoint* point;
  struct rdma_cm_id* descriptor;

  for (number = 0; (number < INSTANT_POINT_COUNT) && (peer->points[peer->round].address.ss_family == AF_UNSPEC); ++ number)
  {
    peer->round ++;
    peer->round %= INSTANT_POINT_COUNT;
  }

  if ((point = peer->points + peer->round) &&
      (point->address.ss_family == AF_UNSPEC))
  {
    // At least one address should be registered
    return -ENOENT;
  }

  if ((rdma_create_id(replicator->channel1, &descriptor, peer, RDMA_PS_TCP)                 != 0) ||
      (rdma_resolve_addr(descriptor, NULL, (struct sockaddr*)&point->address, POLL_TIMEOUT) != 0))
  {
    DestroyDescriptor(descriptor, 1);
    peer->fails ++;
    point->rank ++;
    return -EIO;
  }

  peer->state         = INSTANT_PEER_STATE_CONNECTING;
  peer->descriptor    = descriptor;
  descriptor->context = peer;
  return 0;
}

static void TrackPeerList(struct InstantReplicator* replicator, uint64_t stamp)
{
  struct InstantPeer* peer;
  struct InstantPeer* next;
  struct InstantPeer* previous;

  pthread_mutex_lock(&replicator->lock);

  for (previous = NULL, peer = replicator->peers; peer != NULL; peer = next)
  {
    next = peer->next;

    if ((peer->descriptor == NULL) &&
        (peer->state      == INSTANT_PEER_STATE_DISCONNECTED))
    {
      if (peer->fails >= ATTEMPT_COUNT)
      {
        if (previous != NULL) previous->next    = next;
        else                  replicator->peers = next;

        free(peer);
        continue;
      }

      MakeConnectionAttempt(replicator, peer);
    }

    previous = peer;
  }

  pthread_mutex_unlock(&replicator->lock);
}

// Routines

static void* DoWork(void* closure)
{
  struct InstantReplicator* replicator;
  struct pollfd events[3];
  struct timespec time;
  uint64_t threshold;
  uint64_t stamp;
  ssize_t result;

  replicator = (struct InstantReplicator*)closure;

  events[0].fd      = replicator->handle;
  events[1].fd      = replicator->channel1->fd;
  events[2].fd      = replicator->channel2->fd;
  events[0].events  = POLLIN;
  events[1].events  = POLLIN;
  events[2].events  = POLLIN;
  events[0].revents = 0;
  events[1].revents = 0;
  events[2].revents = 0;

  stamp     = 0ULL;
  threshold = 0ULL;

  pthread_setname_np(replicator->thread, "Replicator");

  ibv_req_notify_cq(replicator->queue, 0);

  while (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
  {
    poll(events, 3, POLL_TIMEOUT);
    clock_gettime(CLOCK_MONOTONIC, &time);

    stamp = time.tv_sec * 1000ULL + time.tv_nsec / 1000000ULL;

    if (events[0].revents & POLLIN)
    {
      HandleApplicationEvent(replicator);
      events[0].revents = 0;
    }

    if (events[1].revents & POLLIN)
    {
      HandleEventChannel(replicator);
      events[1].revents = 0;
    }

    if (events[2].revents & POLLIN)
    {
      HandleCompletionChannel(replicator);
      events[2].revents = 0;
    }

    if (stamp >= threshold)
    {
      TrackPeerList(replicator, stamp);
      threshold = stamp + POLL_TIMEOUT;
    }
  }

  return NULL;
}

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, struct ReliableMonitor* next)
{
  struct InstantReplicator* replicator;
  struct rdma_addrinfo* information;
  struct rdma_addrinfo hint;
  char service[16];

  if (replicator = (struct InstantReplicator*)calloc(1, sizeof(struct InstantReplicator)))
  {
    replicator->super.next     = next;
    replicator->super.closure  = replicator;
    replicator->super.function = HandleMonitorEvent;
    replicator->handle         = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    replicator->secret         = strdup(secret);
    replicator->name           = strdup(name);

    if (identifier == NULL)  uuid_generate(replicator->identifier);
    else                     uuid_copy(replicator->identifier, identifier);

    replicator->handshake.magic = INSTANT_MAGIC;

    getrandom((uint8_t*)&replicator->handshake.nonce, sizeof(uint64_t), 0);
    strncpy(replicator->handshake.name, name, INSTANT_SERVICE_NAME_LENGTH);
    uuid_copy(replicator->handshake.identifier, replicator->identifier);
    HMAC(EVP_sha256(), secret, strlen(secret), (uint8_t*)&replicator->handshake, offsetof(struct InstantHandshakeData, digest), replicator->handshake.digest, NULL);

    pthread_mutex_init(&replicator->lock, NULL);

    snprintf(service, sizeof(service), "%d", port);
    memset(&hint, 0, sizeof(struct rdma_addrinfo));

    hint.ai_flags      = RAI_PASSIVE;
    hint.ai_port_space = RDMA_PS_TCP;
    hint.ai_qp_type    = IBV_QPT_RC;
    information        = NULL;

    if ((replicator->channel1 = rdma_create_event_channel())                    &&
        (rdma_getaddrinfo(NULL, service, &hint, &information)             == 0) &&
        (rdma_create_ep(&replicator->descriptor, information, NULL, NULL) == 0) &&
        (rdma_listen(replicator->descriptor, 16)                          == 0) &&
        (rdma_migrate_id(replicator->descriptor, replicator->channel1)    == 0) &&
        (replicator->context  = replicator->descriptor->verbs)                  &&
        (replicator->channel2 = ibv_create_comp_channel(replicator->context))   &&
        (replicator->domain   = ibv_alloc_pd(replicator->context))              &&
        (replicator->queue    = ibv_create_cq(replicator->context, QUEUE_LENGTH, NULL, replicator->channel2, 0)))
    {
      replicator->parameter.responder_resources = 2;
      replicator->parameter.initiator_depth     = 2;
      replicator->parameter.retry_count         = 5;
      replicator->parameter.rnr_retry_count     = 5;
      replicator->parameter.private_data        = &replicator->handshake;
      replicator->parameter.private_data_len    = sizeof(struct InstantHandshakeData);

      replicator->attribute.cap.max_send_wr     = QUEUE_LENGTH;
      replicator->attribute.cap.max_recv_wr     = QUEUE_LENGTH;
      replicator->attribute.cap.max_send_sge    = 1;
      replicator->attribute.cap.max_recv_sge    = 1;
      replicator->attribute.cap.max_inline_data = 256;
      replicator->attribute.sq_sig_all          = 1;
      replicator->attribute.send_cq             = replicator->queue;
      replicator->attribute.recv_cq             = replicator->queue;
      replicator->attribute.qp_type             = IBV_QPT_RC;

      atomic_store_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_ACTIVE, memory_order_relaxed);

      if (pthread_create(&replicator->thread, NULL, DoWork, replicator) != 0)
      {
        // Startup error, the contents of thread are undefined
        atomic_store_explicit(&replicator->state, 0, memory_order_relaxed);
      }
    }

    rdma_freeaddrinfo(information);
  }

  return replicator;
}

void ReleaseInstantReplicator(struct InstantReplicator* replicator)
{
  struct InstantPeer* peer;

  if (replicator != NULL)
  {
    if (atomic_fetch_and_explicit(&replicator->state, ~INSTANT_REPLICATOR_STATE_ACTIVE, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
    {
      // Thread might be not started
      pthread_join(replicator->thread, NULL);
    }

    while (peer = replicator->peers)
    {
      replicator->peers = peer->next;
      DestroyDescriptor(peer->descriptor, 1);
      free(peer);
    }

    if (replicator->queue      != NULL)  ibv_destroy_cq(replicator->queue);
    if (replicator->domain     != NULL)  ibv_dealloc_pd(replicator->domain);
    if (replicator->channel2   != NULL)  ibv_destroy_comp_channel(replicator->channel2);
    if (replicator->descriptor != NULL)  rdma_destroy_ep(replicator->descriptor);
    if (replicator->channel1   != NULL)  rdma_destroy_event_channel(replicator->channel1);

    pthread_mutex_destroy(&replicator->lock);
    close(replicator->handle);
    free(replicator->secret);
    free(replicator->name);
    free(replicator);
  }
}

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length)
{
  int index;
  struct InstantPeer* peer;
  struct InstantPeer* other;
  struct InstantPoint* point;

  pthread_mutex_lock(&replicator->lock);

  peer = replicator->peers;

  while ((peer != NULL) && (uuid_compare(peer->identifier, identifier) != 0))
  {
    //
    peer = peer->next;
  }

  if (peer == NULL)
  {
    peer = (struct InstantPeer*)calloc(1, sizeof(struct InstantPeer));

    if (peer == NULL)
    {
      pthread_mutex_unlock(&replicator->lock);
      return -ENOMEM;
    }

    uuid_copy(peer->identifier, identifier);

    if (other = replicator->peers)
    {
      peer->next      = other;
      other->previous = peer;
    }

    replicator->peers = peer;
  }

  point = peer->points;

  for (index = 0; index < INSTANT_POINT_COUNT; ++ index)
  {
    if (memcmp(&peer->points[index].address, address, length) == 0)
    {
      pthread_mutex_unlock(&replicator->lock);
      return -EEXIST;
    }

    if ((point->address.ss_family != AF_UNSPEC) &&
        ((peer->points[index].rank > point->rank) ||
         (peer->points[index].address.ss_family == AF_UNSPEC)))
    {
      point = peer->points + index;
      continue;
    }
  }

  memset(point, 0, sizeof(struct InstantPoint));
  memcpy(&point->address, address, length);

  pthread_mutex_unlock(&replicator->lock);
  return 0;
}
