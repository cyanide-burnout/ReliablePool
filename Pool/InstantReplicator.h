#ifndef INSTANTREPLICATOR_H
#define INSTANTREPLICATOR_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "ReliablePool.h"
#include "ReliableTracker.h"
#include "ReliableIndexer.h"

#include <uuid.h>
#include <liburing.h>
#include <openssl/sha.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>

#ifdef __cplusplus
extern "C"
{
#endif

// Protocol

#define INSTANT_MAGIC                0xe29a
#define INSTANT_SERVICE_NAME_LENGTH  16

#define INSTANT_TYPE_NOTIFY     1
#define INSTANT_TYPE_RETREIVE   2

struct InstantHandshakeData
{
  uint16_t magic;                          // 2
  uint16_t nonce;                          // 4
  uuid_t identifier;                       // 20
  char name[INSTANT_SERVICE_NAME_LENGTH];  // 36
  uint8_t digest[SHA_DIGEST_LENGTH];       // 56
} __attribute__((packed));

struct InstantCookieData
{
  uint32_t length;                         // Length of whole chunk
  uint32_t count;                          // Count of registered regions
  char name[RELIABLE_MEMORY_NAME_LENGTH];  // Name of pool
  uint32_t keys[0];                        // List of remote keys
} __attribute__((packed));

struct InstantBlockData
{
  uuid_t identifier;  // Block UUID
  uint64_t address;   // ReliableBlock::mark
  uint32_t length;    // sizeof(struct ReliableBlock) - offsetof(struct ReliableBlock, mark) + block->length
  uint64_t mark;      //
} __attribute__((packed));

struct InstantHeaderData
{
  uuid_t identifier;  // Local instance UUID
  uint16_t type;      // INSTANT_TYPE_*
  uint32_t task;      // Task ID (for IBV_WC_RECV_RDMA_WITH_IMM) or UINT32_MAX
} __attribute__((packed));

// Replicator

#define INSTANT_REPLICATOR_STATE_ACTIVE   (1 << 0)
#define INSTANT_REPLICATOR_STATE_FAILURE  (1 << 1)
#define INSTANT_REPLICATOR_STATE_HOLD     (1 << 2)
#define INSTANT_REPLICATOR_STATE_READY    (1 << 3)

#define INSTANT_PEER_STATE_DISCONNECTED  0
#define INSTANT_PEER_STATE_CONNECTING    1
#define INSTANT_PEER_STATE_CONNECTED     2

#define INSTANT_TASK_TYPE_SYNCING  0
#define INSTANT_TASK_TYPE_READING  INSTANT_TYPE_NOTIFY
#define INSTANT_TASK_TYPE_WRITING  INSTANT_TYPE_RETREIVE

#define INSTANT_TASK_STATE_IDLE             0
#define INSTANT_TASK_STATE_PROGRESS         1
#define INSTANT_TASK_STATE_WAIT_DATA        2
#define INSTANT_TASK_STATE_WAIT_HOLD        3
#define INSTANT_TASK_STATE_WAIT_BUFFER      4
#define INSTANT_TASK_STATE_WAIT_COMPLETION  5

#define INSTANT_POINT_COUNT  8    // Very optimistic (usually reserve 2-4 legs)
#define INSTANT_CARD_COUNT   128  // More HCAs are unexpected (who uses more than two?)

#define INSTANT_QUEUE_LENGTH   2048  // Must be a power of two
#define INSTANT_BUFFER_LENGTH  4096  // Corresponds to InfiniBand payload MTU (RoCE jumbo-frames have greater length)

#define INSTANT_TASK_ALIGNMENT            128ULL
#define INSTANT_BATCH_LENGTH_LIMIT        (INSTANT_BUFFER_LENGTH / sizeof(struct InstantBlockData) + 1)
#define INSTANT_MESSAGE_LENGTH_THRESHOLD  (INSTANT_BUFFER_LENGTH - 2 * sizeof(struct InstantBlockData))

#define INSTANT_TASK_SLOT_MASK     (INSTANT_TASK_ALIGNMENT - 1ULL)
#define INSTANT_TASK_ADDRESS_MASK  (~INSTANT_TASK_SLOT_MASK)

struct InstantSharedBuffer
{
  uint32_t number;         // Buffer number
  uint32_t length;         // Length of data
  ATOMIC(uint32_t) tag;    // Generation tag
  ATOMIC(uint64_t) next;   // Next buffer on stack
  ATOMIC(uint32_t) count;  // Reference count

  union
  {
    uint8_t data[INSTANT_BUFFER_LENGTH];
    uint64_t values[INSTANT_BATCH_LENGTH_LIMIT];
  };
};

struct InstantSharedBufferList
{
  ATOMIC(uint64_t) stack;
  struct InstantSharedBuffer data[INSTANT_QUEUE_LENGTH];
};

struct InstantSendingQueue
{
  ATOMIC(uint32_t) head;
  ATOMIC(uint32_t) tail;
  ATOMIC(uint32_t) count;
  ATOMIC(struct InstantSharedBuffer*) data[INSTANT_QUEUE_LENGTH];
};

struct InstantRequestItem
{
  struct InstantRequestItem* next;
  struct ibv_send_wr request;
  struct ibv_sge element;
};

struct InstantRequestQueue
{
  struct InstantRequestItem* head;
  struct InstantRequestItem* tail;
};

struct InstantCookie
{
  struct InstantCookie* previous;
  struct InstantCookie* next;

  uint32_t expiration;                         // Expication time in ticks (cleanup collection)
  struct ReliableShare* share;                 // Associated share
  struct ibv_mr* regions[INSTANT_CARD_COUNT];  // List of regions (in order of InstantCard::number)

  struct InstantCookieData data;               // Cached InstantCookieData
  uint32_t reserved[INSTANT_CARD_COUNT];       //
};

struct InstantCard
{
  struct InstantCard* previous;
  struct InstantCard* next;

  struct ibv_comp_channel* channel;   // Completion channel
  struct ibv_context* context;        // Verbs context of device
  struct ibv_srq* queue1;             // Shared receive queue
  struct ibv_cq* queue2;              // Completion queue for both send() and recv()
  struct ibv_pd* domain;              // Protection domaini
  struct ibv_mr* region1;             // Receiving buffers
  struct ibv_mr* region2;             // Sending buffers

  uint32_t number;                    // Number of InstantCard, used as an index in cookie, also sent as IMM
  struct ibv_qp_init_attr attribute;  // Attributes for rdma_create_qp()

  uint8_t buffers[INSTANT_QUEUE_LENGTH * 2][INSTANT_BUFFER_LENGTH];
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
  struct InstantRequestQueue queue;

  uint32_t state;  // INSTANT_PEER_STATE_*
  uint32_t round;  // Round-robin index of points
  uint32_t fails;  // Connection failures count

  uuid_t identifier;
  struct InstantPoint points[INSTANT_POINT_COUNT];
};

struct InstantSyncingTaskData  // INSTANT_TASK_TYPE_SYNCING
{
  uint32_t cursor;
  uint32_t* list;
};

struct InstantTransferTaskData  // INSTANT_TASK_TYPE_READING, INSTANT_TASK_TYPE_WRITING
{
  uint32_t key;
  uint32_t task;
  uint32_t count;
  struct InstantSharedBuffer* buffer;
  struct InstantBlockData list[INSTANT_BATCH_LENGTH_LIMIT];
};

struct InstantTask
{
  struct InstantTask* previous;
  struct InstantTask* next;

  int type;
  uint32_t state;
  uint32_t number;
  struct InstantPeer* peer;
  char name[RELIABLE_MEMORY_NAME_LENGTH];

  union
  {
    struct InstantSyncingTaskData syncing;
    struct InstantTransferTaskData transfer;
  };
};

struct InstantTaskList
{
  uint32_t count;            // Count of tasks that require INSTANT_REPLICATOR_STATE_HOLD
  uint32_t number;           // Task number counter
  struct InstantTask* head;  //
  struct InstantTask* tail;  //
};

struct InstantReplicator
{
  struct ReliableMonitor super;
  struct ReliableIndexer* indexer;

  char* name;
  char* secret;
  uint32_t limit;
  uuid_t identifier;

  struct io_uring ring;
  struct rdma_cm_id* descriptor;
  struct rdma_event_channel* channel;

  pthread_t thread;
  pthread_mutex_t lock;
  ATOMIC(uint32_t) state;
  struct InstantCard* cards;
  struct InstantPeer* peers;
  struct InstantTask* tasks;
  struct InstantCookie* cookies;
  struct InstantRequestItem* items;

  struct rdma_conn_param parameter;
  struct InstantHandshakeData handshake;

  struct InstantTaskList schedule;
  struct InstantSendingQueue queue;
  struct InstantSharedBufferList buffers;
};

typedef int (*ExecuteInstantTaskFunction)(struct InstantReplicator* replicator, struct InstantTask* task);

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, uint32_t limit, struct ReliableMonitor* next);
void ReleaseInstantReplicator(struct InstantReplicator* replicator);

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length);

#ifdef __cplusplus
}
#endif

#endif
