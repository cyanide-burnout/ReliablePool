#include "InstantReplicator.h"

#include <poll.h>
#include <time.h>
#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <sys/random.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "ReliableTracker.h"
#include "ReliableIndexer.h"

#define POLL_TIMEOUT   200   // Milliseconds
#define QUEUE_LENGTH   4096  //
#define ATTEMPT_COUNT  128   //

#define COUNT(array)  (sizeof(array) / sizeof(array[0]))

_Static_assert(sizeof(struct InstantHandshakeData) <= 56, "private_data_len for RDMA_PS_TCP must be maximum 56 bytes in length");

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
  /*
  int result;
  void* context;
  struct ibv_cq* queue;
  struct ibv_wc* completion;
  struct ibv_wc completions[32];

  while (ibv_get_cq_event(replicator->channel2, &queue, &context) == 0)
  {
    ibv_ack_cq_events(queue, 1);
    ibv_req_notify_cq(queue, 0);

    while ((result = ibv_poll_cq(queue, COUNT(completions), completions)) > 0)
    {
      for (completion = completions; completion < (completions + result); ++ completion)
      {
        // if (completion->status != IBV_WC_SUCCESS)
        switch (completion->opcode)
        {
          case IBV_WC_RECV:        break;
          case IBV_WC_SEND:        break;
          case IBV_WC_RDMA_READ:   break;
          case IBV_WC_RDMA_WRITE:  break;
        }
      }
    }
  }
  */
}

// Connection tracking

static struct InstantCard* FindOrCreateCard(struct InstantReplicator* replicator, struct ibv_context* context)
{
  struct InstantCard* card;

  for (card = replicator->cards; (card != NULL) && (card->context != context); card = card->next);

  if ((card == NULL) &&
      (card  = (struct InstantCard*)calloc(1, sizeof(struct InstantCard))))
  {
    if ((card->channel = ibv_create_comp_channel(context))  &&
        (card->domain  = ibv_alloc_pd(context))             &&
        (card->queue   = ibv_create_cq(context, QUEUE_LENGTH, NULL, card->channel, 0)))
    {
      card->attribute.cap.max_send_wr     = QUEUE_LENGTH;
      card->attribute.cap.max_recv_wr     = QUEUE_LENGTH;
      card->attribute.cap.max_send_sge    = 1;
      card->attribute.cap.max_recv_sge    = 1;
      card->attribute.cap.max_inline_data = 256;
      card->attribute.sq_sig_all          = 1;
      card->attribute.send_cq             = card->queue;
      card->attribute.recv_cq             = card->queue;
      card->attribute.qp_type             = IBV_QPT_RC;
      card->context                       = context;
      card->next                          = replicator->cards;
      replicator->cards                   = card;
    }
    else
    {
      if (card->queue    != NULL)  ibv_destroy_cq(card->queue);
      if (card->domain   != NULL)  ibv_dealloc_pd(card->domain);
      if (card->channel  != NULL)  ibv_destroy_comp_channel(card->channel);
      free(card);
      card = NULL;
    }
  }

  return card;
}

static void RemoveCard(struct InstantReplicator* replicator, struct InstantCard* card)
{
  struct InstantCard* other;

	if (other = card->next)      other->previous   = card->previous;
  if (other = card->previous)  other->next       = card->next;
  else                         replicator->cards = card->next;

  ibv_destroy_cq(card->queue);
  ibv_dealloc_pd(card->domain);
  ibv_destroy_comp_channel(card->channel);
  free(card);
}

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
  struct InstantCard* card;
  struct InstantPeer* peer;
  struct InstantHandshakeData* handshake;
  uint8_t digest[SHA_DIGEST_LENGTH];

  card      = FindOrCreateCard(replicator, descriptor->verbs);
  peer      = NULL;
  handshake = NULL;

  if ((card      != NULL) &&
      (parameter != NULL) &&
      (parameter->private_data_len == sizeof(struct InstantHandshakeData)) &&
      (handshake = (struct InstantHandshakeData*)parameter->private_data)  &&
      (handshake->magic == INSTANT_MAGIC) &&
      (memcmp(handshake->name, replicator->handshake.name, INSTANT_SERVICE_NAME_LENGTH) == 0) &&
      (HMAC(EVP_sha1(), replicator->secret, strlen(replicator->secret), (uint8_t*)handshake, offsetof(struct InstantHandshakeData, digest), digest, NULL) != NULL) &&
      (memcmp(handshake->digest, digest, SHA_DIGEST_LENGTH) == 0))
  {
    pthread_mutex_lock(&replicator->lock);
    for (peer = replicator->peers; (peer != NULL) && (uuid_compare(handshake->identifier, peer->identifier) != 0); peer = peer->next);
    pthread_mutex_unlock(&replicator->lock);
  }

  if ((peer == NULL) ||
      (peer->state != INSTANT_PEER_STATE_DISCONNECTED) ||
      (rdma_create_qp(descriptor, card->domain, &card->attribute) != 0) ||
      (rdma_accept(descriptor, &replicator->parameter) != 0))
  {
    rdma_reject(descriptor, NULL, 0);
    return -1;
  }

  peer->state         = INSTANT_PEER_STATE_CONNECTING;
  peer->descriptor    = descriptor;
  peer->card          = card;
  descriptor->context = peer;
  return 0;
}

static int HandleEstablished(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor)
{
  struct InstantPeer* peer;

  printf("HandleEstablished id=%p\n", descriptor);

  peer = (struct InstantPeer*)descriptor->context;

  peer->state      = INSTANT_PEER_STATE_CONNECTED;
  peer->fails      = 0;
  peer->points[peer->round].rank = 0;

  return 0;
}

static int HandleDisconnected(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor, int reason)
{
  struct InstantPeer* peer;

  printf("HandleDisconnected id=%p\n", descriptor);

  if ((peer = (struct InstantPeer*)descriptor->context) &&
      (peer->descriptor == descriptor))
  {
    peer->state      = INSTANT_PEER_STATE_DISCONNECTED;
    peer->descriptor = NULL;
    peer->card       = NULL;

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
  struct InstantCard* card;
  struct InstantPeer* peer;

  card       = FindOrCreateCard(replicator, descriptor->verbs);
  peer       = (struct InstantPeer*)descriptor->context;
  peer->card = card;

  if ((card == NULL) ||
      (rdma_create_qp(descriptor, card->domain, &card->attribute) != 0) ||
      (rdma_connect(descriptor, &replicator->parameter)           != 0))
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

  if (rdma_get_cm_event(replicator->channel, &event) == 0)
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
      // case RDMA_CM_EVENT_DEVICE_REMOVAL:
    }

    rdma_ack_cm_event(event);
    DestroyDescriptor(descriptor, result);
  }
}

static int TryConnect(struct InstantReplicator* replicator, struct InstantPeer* peer)
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

  descriptor = NULL;

  if ((rdma_create_id(replicator->channel, &descriptor, peer, RDMA_PS_TCP)                  != 0) ||
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

static void TrackPeerList(struct InstantReplicator* replicator)
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

      TryConnect(replicator, peer);
    }

    previous = peer;
  }

  pthread_mutex_unlock(&replicator->lock);
}

// Routines

static void* DoWork(void* closure)
{
  struct InstantReplicator* replicator;
  struct itimerspec interval;
  struct pollfd events[4];
  uint64_t value;
  int handle;

  replicator = (struct InstantReplicator*)closure;
  handle     = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);

  events[0].fd      = handle;
  events[1].fd      = replicator->handle;
  events[2].fd      = replicator->channel->fd;
  // events[3].fd      = replicator->channel2->fd;
  events[0].events  = POLLIN;
  events[1].events  = POLLIN;
  events[2].events  = POLLIN;
  events[3].events  = POLLIN;
  events[0].revents = 0;
  events[1].revents = 0;
  events[2].revents = 0;
  events[3].revents = 0;

  interval.it_interval.tv_nsec = POLL_TIMEOUT * 1000000ULL;
  interval.it_interval.tv_sec  = 0;
  interval.it_value.tv_nsec    = interval.it_interval.tv_nsec;
  interval.it_value.tv_sec     = interval.it_interval.tv_sec;

  pthread_setname_np(replicator->thread, "Replicator");
  timerfd_settime(handle, 0, &interval, NULL);
  // ibv_req_notify_cq(replicator->queue, 0);

  while (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
  {
    poll(events, 3, POLL_TIMEOUT);

    if (events[0].revents & POLLIN)
    {
      read(handle, &value, sizeof(uint64_t));
      TrackPeerList(replicator);
      events[0].revents = 0;
    }

    if (events[1].revents & POLLIN)
    {
      read(replicator->handle, &value, sizeof(uint64_t));
      HandleApplicationEvent(replicator);
      events[1].revents = 0;
    }

    if (events[2].revents & POLLIN)
    {
      HandleEventChannel(replicator);
      events[2].revents = 0;
    }

    if (events[3].revents & POLLIN)
    {
      HandleCompletionChannel(replicator);
      events[3].revents = 0;
    }
  }

  close(handle);

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

    getrandom((uint8_t*)&replicator->handshake.nonce, sizeof(uint32_t), 0);
    strncpy(replicator->handshake.name, name, INSTANT_SERVICE_NAME_LENGTH);
    uuid_copy(replicator->handshake.identifier, replicator->identifier);
    HMAC(EVP_sha1(), secret, strlen(secret), (uint8_t*)&replicator->handshake, offsetof(struct InstantHandshakeData, digest), replicator->handshake.digest, NULL);

    pthread_mutex_init(&replicator->lock, NULL);

    snprintf(service, sizeof(service), "%d", port);
    memset(&hint, 0, sizeof(struct rdma_addrinfo));

    hint.ai_flags      = RAI_PASSIVE;
    hint.ai_port_space = RDMA_PS_TCP;
    hint.ai_qp_type    = IBV_QPT_RC;
    information        = NULL;

    if ((replicator->channel = rdma_create_event_channel())                     &&
        (rdma_getaddrinfo(NULL, service, &hint, &information)             == 0) &&
        (rdma_create_ep(&replicator->descriptor, information, NULL, NULL) == 0) &&
        (rdma_listen(replicator->descriptor, 16)                          == 0) &&
        (rdma_migrate_id(replicator->descriptor, replicator->channel)     == 0))
    {
      replicator->parameter.responder_resources = 2;
      replicator->parameter.initiator_depth     = 2;
      replicator->parameter.retry_count         = 5;
      replicator->parameter.rnr_retry_count     = 5;
      replicator->parameter.private_data        = &replicator->handshake;
      replicator->parameter.private_data_len    = sizeof(struct InstantHandshakeData);

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
  struct InstantCard* card;

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

    while (card = replicator->cards)
    {
      //
      RemoveCard(replicator, card);
    }

    if (replicator->descriptor != NULL)  rdma_destroy_ep(replicator->descriptor);
    if (replicator->channel    != NULL)  rdma_destroy_event_channel(replicator->channel);

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

  for (peer = replicator->peers; (peer != NULL) && (uuid_compare(peer->identifier, identifier) != 0); peer = peer->next);

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
