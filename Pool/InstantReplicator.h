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

#define INSTANT_MAGIC                0x9aa1
#define INSTANT_SERVICE_NAME_LENGTH  16

#define INSTANT_TYPE_CLOCK      1
#define INSTANT_TYPE_NOTIFY     2
#define INSTANT_TYPE_RETRIEVE   3
#define INSTANT_TYPE_COMPLETE   4
#define INSTANT_TYPE_REMOVE     5
#define INSTANT_TYPE_CREDIT     6
#define INSTANT_TYPE_USER       7

struct InstantHandshakeData
{
  uint16_t magic;                          // 2
  uint16_t nonce;                          // 4
  uuid_t identifier;                       // 20
  char name[INSTANT_SERVICE_NAME_LENGTH];  // 36
  uint8_t digest[SHA_DIGEST_LENGTH];       // 56
} __attribute__((packed));

struct InstantRemovalData
{
  char name[RELIABLE_MEMORY_NAME_LENGTH];  // Name of pool
  uuid_t identifier;                       // Block UUID
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
  uint64_t hint;      // Version (INSTANT_TYPE_NOTIFY) or token that completes the transfer (INSTANT_TYPE_RETRIEVE)
  uint64_t mark;      //
} __attribute__((packed));

struct InstantCreditData
{
  uint32_t window;   // Count of messages the sender of this report accepts in flight from the peer
  uint32_t applied;  // Window of the peer the sender of this report is applying
} __attribute__((packed));

struct InstantHeaderData
{
  uuid_t identifier;  // Local instance UUID
  uint16_t type;      // INSTANT_TYPE_*
  uint32_t task;      // Task ID (for IBV_WC_RECV_RDMA_WITH_IMM) or UINT32_MAX
} __attribute__((packed));

// Replicator

#define RELIABLE_MONITOR_BLOCK_DAMAGE   13
#define RELIABLE_MONITOR_BLOCK_ARRIVAL  14
#define RELIABLE_MONITOR_BLOCK_REMOVAL  15

#define INSTANT_REPLICATOR_EVENT_FLUSH         0
#define INSTANT_REPLICATOR_EVENT_CONNECTED     1
#define INSTANT_REPLICATOR_EVENT_DISCONNECTED  2
#define INSTANT_REPLICATOR_EVENT_USER_MESSAGE  3

#define INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE  (1U << 0)

#define INSTANT_REPLICATOR_STATE_ACTIVE   (1U << 0)
#define INSTANT_REPLICATOR_STATE_FAILURE  (1U << 1)
#define INSTANT_REPLICATOR_STATE_LOCK     (1U << 2)
#define INSTANT_REPLICATOR_STATE_READY    (1U << 3)

#define INSTANT_PEER_STATE_DISCONNECTED  0
#define INSTANT_PEER_STATE_CONNECTING    1
#define INSTANT_PEER_STATE_CONNECTED     2
#define INSTANT_PEER_STATE_FAILED        3

#define INSTANT_TASK_TYPE_SYNCING   0
#define INSTANT_TASK_TYPE_CLOCKING  INSTANT_TYPE_CLOCK
#define INSTANT_TASK_TYPE_READING   INSTANT_TYPE_NOTIFY
#define INSTANT_TASK_TYPE_WRITING   INSTANT_TYPE_RETRIEVE

#define INSTANT_TASK_STATE_IDLE             0
#define INSTANT_TASK_STATE_PROGRESS         1
#define INSTANT_TASK_STATE_WAIT_DATA        2
#define INSTANT_TASK_STATE_WAIT_LOCK        3
#define INSTANT_TASK_STATE_WAIT_BUFFER      4
#define INSTANT_TASK_STATE_WAIT_COMPLETION  5

#define INSTANT_ENTRY_FLAG_FETCHED  (1U << 0)  // Optimistic read is posted
#define INSTANT_ENTRY_FLAG_DAMAGED  (1U << 1)  // Content was overwritten by a rejected transfer

#define INSTANT_POINT_COUNT  8   // Very optimistic (usually 2-4 legs reserved)
#define INSTANT_CARD_COUNT   64  // More HCAs are unexpected (who uses more than 8?)

#define INSTANT_QUEUE_LENGTH    2048  // Must be a power of two
#define INSTANT_BUFFER_LENGTH   4096  // Corresponds to InfiniBand payload MTU (RoCE jumbo-frames have greater length)
#define INSTANT_RESERVE_COUNT   256   // Shared buffers the application thread cannot take, kept for the replicator thread
#define INSTANT_ATOMIC_COUNT    16    // RDMA READ and atomic operations in flight per QP, limited by the weakest card and the peer
#define INSTANT_BARRIER_COUNT   1024  // Entries of reading tasks served by one barrier, a backlog is split into several barriers

#define INSTANT_CREDIT_SHIFT    6     // imm_data of SEND carries the card number in the low bits and the count of received messages above
#define INSTANT_CREDIT_RESERVE  64    // Receiving buffers kept for credit reports, at most one is in flight per peer
#define INSTANT_MESSAGE_COUNT   512   // Queued REMOVE and USER messages, beyond it they are refused
#define INSTANT_CREDIT_FREE     0     // Sent without a credit (INSTANT_TYPE_CREDIT, INSTANT_TYPE_USER), still counted
#define INSTANT_CREDIT_ACQUIRE  1     // Sent only within the window of the peer
#define INSTANT_CREDIT_HELD     2     // The credit was taken in advance

#define INSTANT_TOKEN_RANGE               (1 << 20)                                                      // Tokens reserved by one update of ReliableMemory::floor
#define INSTANT_BATCH_LENGTH_LIMIT        (INSTANT_BUFFER_LENGTH / sizeof(struct InstantBlockData) + 1)  //
#define INSTANT_MESSAGE_LENGTH_THRESHOLD  (INSTANT_BUFFER_LENGTH - 2 * sizeof(struct InstantBlockData))  //

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
  ATOMIC(uint32_t) count;    // Count of free buffers
  ATOMIC(uint32_t) waiters;  // Count of threads waiting for a buffer
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
  struct InstantCookie* next;
  uint32_t expiration;                         // Expiration time in ticks (cleanup collection)
  uint64_t token;                              // Next token counter
  uint64_t limit;                              // End of reserved range of tokens

  struct ReliableShare* share;                 // Associated share
  struct ibv_mr* regions[INSTANT_CARD_COUNT];  // List of regions (in order of InstantCard::number)

  struct InstantCookieData data;               // Cached InstantCookieData
  uint32_t reserved[INSTANT_CARD_COUNT];       //
};

struct InstantRemoval
{
  struct InstantRemoval* next;
  uint32_t expiration;

  struct InstantRemovalData data;
};

struct InstantRemovalQueue
{
  struct InstantRemoval* stack;
  struct InstantRemoval* head;
  struct InstantRemoval* tail;
};

struct InstantMessageQueue  // INSTANT_TYPE_REMOVE and INSTANT_TYPE_USER delivered in order to every peer within its credit
{
  struct InstantSharedBuffer* head;  // Owned by the replicator thread, linked through InstantSharedBuffer::next while allocated
  struct InstantSharedBuffer* tail;  //
  uint32_t sequence;                 // Sequence of the head
  uint32_t count;                    // Count of messages in the list
  ATOMIC(uint32_t) length;           // Count of messages, including those still in the sending queue
  ATOMIC(uint32_t) waiters;          // Count of threads waiting for a place in the queue
};

struct InstantBarrier
{
  uint32_t boundary;         // Number of the first reading task left for the next barrier
  uint32_t parked;           // The last barrier was released while the application was parked
  uint32_t released;         // Value of returns when the last barrier was released
  ATOMIC(uint32_t) returns;  // Count of returns of the application from a barrier
};

struct InstantLoss
{
  ATOMIC(uint32_t) count;  // Notifications dropped on the application thread
  uint32_t last;           // Value of count already accounted to the peers
};

struct InstantCookieQueue
{
  struct InstantCookie* head;
  struct InstantCookie* tail;
};

struct InstantCard
{
  struct InstantCard* previous;
  struct InstantCard* next;

  struct ibv_comp_channel* channel;   // Completion channel
  struct ibv_context* context;        // Verbs context of device
  struct ibv_srq* queue1;             // Shared receive queue
  struct ibv_cq* queue2;              // Completion queue for both send() and recv()
  struct ibv_pd* domain;              // Protection domain
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

struct InstantCredit
{
  uint32_t sent;                       // Count of messages that consume a receiving buffer of the peer
  uint32_t acknowledged;               // Count of them the peer has received
  uint32_t window;                     // Window announced by the peer
  uint32_t echoed;                     // Value of window last reported to the peer as applied
  uint32_t received;                   // Count of messages received from the peer
  uint32_t reported;                   // Value of received last reported to the peer
  uint32_t advertised;                 // Window last announced to the peer
  uint32_t applied;                    // Window the peer reports applying
  uint32_t committed;                  // Largest window the peer may still apply until it confirms the last one
  uint32_t stalled;                    // Value of received when the window was last seen open or moving
  uint32_t expiration;                 // Tick until which an exhausted window must move
  struct InstantSharedBuffer* buffer;  // INSTANT_TYPE_CREDIT in flight
};

struct InstantPeer
{
  struct InstantPeer* previous;
  struct InstantPeer* next;

  struct rdma_cm_id* descriptor;

  struct InstantCard* card;
  struct InstantRequestQueue queue;
  struct InstantCredit credit;

  uint32_t state;         // INSTANT_PEER_STATE_*
  uint32_t round;         // Round-robin index of points
  uint32_t fails;         // Connection failures count
  uint32_t pending;       // SEND work requests in flight
  uint32_t lost;          // Count of notifications not sent to the peer
  uint32_t last;          // Value of lost at the start of the last syncing
  uint32_t delivered;     // Sequence of the next queued message to send
  int64_t vector;         // Clock vector in units of mark epoch

  uuid_t identifier;
  struct InstantPoint points[INSTANT_POINT_COUNT];
};

struct InstantSyncingTaskData  // INSTANT_TASK_TYPE_SYNCING
{
  uint32_t cursor;
  uint32_t* list;
};

struct InstantEntryState
{
  uint32_t number;  // Number of the local block or UINT32_MAX when the entry is done
  uint32_t flags;   // INSTANT_ENTRY_FLAG_*
  uint64_t hint;    // Saved hint of the local block
  uint64_t mark;    // Expected mark of the local block while its content is untouched
  uint64_t token;   // Token of the current attempt
};

struct InstantTransferTaskData  // INSTANT_TASK_TYPE_READING, INSTANT_TASK_TYPE_WRITING
{
  uint32_t key;
  uint32_t task;
  uint32_t code;
  uint32_t count;
  uint32_t attempt;
  struct InstantSharedBuffer* buffer;
  struct InstantEntryState states[INSTANT_BATCH_LENGTH_LIMIT];
  struct InstantBlockData entries[INSTANT_BATCH_LENGTH_LIMIT];
};

struct InstantTask
{
  struct InstantTask* previous;
  struct InstantTask* next;

  int type;
  uint32_t state;
  uint32_t number;
  uint32_t expiration;
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
  uint32_t count;            // Count of tasks that require INSTANT_REPLICATOR_STATE_LOCK
  uint32_t number;           // Task number counter
  struct InstantTask* head;  //
  struct InstantTask* tail;  //
};

typedef void (*HandleInstantEventFunction)(int event, struct InstantPeer* peer, const char* data, int parameter, void* closure);

struct InstantReplicator
{
  struct ReliableMonitor super;
  struct ReliableIndexer* indexer;
  HandleInstantEventFunction function;

  char* name;
  char* secret;
  void* closure;
  uint32_t options;
  uint32_t timeout;
  uuid_t identifier;

  struct io_uring ring;
  struct rdma_cm_id* descriptor;
  struct rdma_event_channel* channel;

  size_t size;
  uint32_t tick;
  pthread_t thread;
  pthread_mutex_t lock;
  ATOMIC(uint32_t) state;
  struct InstantLoss loss;
  struct InstantBarrier barrier;

  struct InstantCard* cards;
  struct InstantPeer* peers;
  struct InstantTask* tasks;
  struct InstantRequestItem* items;

  struct rdma_conn_param parameter;
  struct InstantHandshakeData handshake;

  struct InstantTaskList schedule;
  struct InstantSendingQueue queue;
  struct InstantCookieQueue cookies;
  struct InstantMessageQueue messages;
  struct InstantRemovalQueue removals;
  struct InstantSharedBufferList buffers;
};

typedef int (*ExecuteInstantTaskFunction)(struct InstantReplicator* replicator, struct InstantTask* task);

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, uint32_t options, uint32_t timeout, HandleInstantEventFunction function, void* closure, struct ReliableMonitor* next);
void ReleaseInstantReplicator(struct InstantReplicator* replicator);

int FlushInstantReplicator(struct InstantReplicator* replicator);
int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length);
int TransmitInstantReplicatorUserMessage(struct InstantReplicator* replicator, const char* data, uint32_t length, int wait);

#ifdef __cplusplus
}
#endif

#endif
