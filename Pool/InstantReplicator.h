#ifndef INSTANTREPLICATOR_H
#define INSTANTREPLICATOR_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "ReliablePool.h"

#include <uuid.h>
#include <openssl/sha.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C"
{
#endif

// Protocol

#define INSTANT_MAGIC                0xe29a4b2d
#define INSTANT_SERVICE_NAME_LENGTH  12

struct InstantHandshakeData
{
  uint32_t magic;                          // 4
  uint32_t nonce;                          // 8
  uuid_t identifier;                       // 24
  char name[INSTANT_SERVICE_NAME_LENGTH];  // 36
  uint8_t digest[SHA_DIGEST_LENGTH];       // 56
} __attribute__((packed));

// Replicator

#define INSTANT_REPLICATOR_STATE_ACTIVE   (1 << 0)
#define INSTANT_REPLICATOR_STATE_FAILURE  (1 << 1)

#define INSTANT_PEER_STATE_DISCONNECTED 0
#define INSTANT_PEER_STATE_CONNECTING   1
#define INSTANT_PEER_STATE_CONNECTED    2

#define INSTANT_POINT_COUNT  8

struct InstantCard
{
  struct InstantCard* previous;
  struct InstantCard* next;

  struct ibv_comp_channel* channel;
  struct ibv_context* context;
  struct ibv_pd* domain;
  struct ibv_cq* queue;

  struct ibv_qp_init_attr attribute;
};

struct InstantPoint
{
  struct sockaddr_storage address;
  uint32_t rank;
};

struct InstantPeer
{
  struct InstantPeer* previous;
  struct InstantPeer* next;

  struct rdma_cm_id* descriptor;
  struct InstantCard* card;

  uint32_t state;  // INSTANT_PEER_STATE_*
  uint32_t round;  // Round-robin index of points
  uint32_t fails;  // Connection failures count

  uuid_t identifier;
  struct InstantPoint points[INSTANT_POINT_COUNT];
};

struct InstantReplicator
{
  struct ReliableMonitor super;

  struct rdma_cm_id* descriptor;
  struct rdma_event_channel* channel;

  int handle;
  pthread_t thread;
  pthread_mutex_t lock;
  ATOMIC(uint32_t) state;
  struct InstantCard* cards;
  struct InstantPeer* peers;

  char* name;
  char* secret;
  uuid_t identifier;

  struct rdma_conn_param parameter;
  struct InstantHandshakeData handshake;
};

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, struct ReliableMonitor* next);
void ReleaseInstantReplicator(struct InstantReplicator* replicator);

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length);

#ifdef __cplusplus
}
#endif

#endif
