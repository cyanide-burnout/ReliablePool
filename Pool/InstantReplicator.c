#include "InstantReplicator.h"

#include <poll.h>
#include <time.h>
#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <sys/random.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include "CRC32C.h"

#define RING_TAG_TIMEOUT        (LIBURING_UDATA_TIMEOUT - 1ULL)
#define RING_TAG_READY_STATE    (LIBURING_UDATA_TIMEOUT - 2ULL)
#define RING_TAG_EVENT_CHANNEL  (LIBURING_UDATA_TIMEOUT - 3ULL)
#define RING_TAG_SENDING_QUEUE  (LIBURING_UDATA_TIMEOUT - 4ULL)

#define GENERIC_POLL_TIMEOUT       200
#define COOKIE_EXPIRATION_COUNT    (60000 / GENERIC_POLL_TIMEOUT)
#define REMOVAL_EXPIRATION_COUNT   (10000 / GENERIC_POLL_TIMEOUT)
#define CONNECTION_ATTEMPT_COUNT   128
#define READING_ATTEMPT_COUNT      3

#define COUNT(array)  (sizeof(array) / sizeof(array[0]))

#ifndef IORING_ENTER_NO_IOWAIT
#define io_uring_set_iowait(ring, value)
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define WATCH(address)  ((uint32_t*)(address))
#else
#define WATCH(address)  ((uint32_t*)(address) + 1)
#endif

_Static_assert(sizeof(struct InstantHandshakeData) <= 56,                                                 "private_data_len for RDMA_PS_TCP must be maximum 56 bytes in length");
_Static_assert((sizeof(struct InstantSharedBuffer)             % sizeof(uint64_t)) == 0,                  "Size of InstantSharedBuffer must be 64-bit aligned");
_Static_assert((offsetof(struct InstantSharedBuffer, values)   % sizeof(uint64_t)) == 0,                  "InstantSharedBuffer.values must be 64-bit aligned");
_Static_assert((offsetof(struct InstantSharedBufferList, data) % sizeof(uint64_t)) == 0,                  "InstantSharedBufferList.data must be 64-bit aligned");
_Static_assert((offsetof(struct InstantReplicator, buffers)    % sizeof(uint64_t)) == 0,                  "InstantReplicator.buffers must be 64-bit aligned");
_Static_assert((INSTANT_QUEUE_LENGTH != 0) && ((INSTANT_QUEUE_LENGTH & (INSTANT_QUEUE_LENGTH - 1)) == 0), "INSTANT_QUEUE_LENGTH must be a power of two");

// Helpers

static void CallEventFunction(struct InstantReplicator* replicator, int event, struct InstantPeer* peer, const char* data, int parameter)
{
  if (replicator->function != NULL)
  {
    //
    replicator->function(event, peer, data, parameter, replicator->closure);
  }
}

// Buffers, queues

static inline uint32_t PushSharedBuffer(struct InstantSharedBufferList* list, struct InstantSharedBuffer* buffer)
{
  uint64_t current;
  uint64_t next;

  next = ((uint64_t)atomic_fetch_add_explicit(&buffer->tag, 1, memory_order_relaxed) << 32) | buffer->number;

  do
  {
    current = atomic_load_explicit(&list->stack, memory_order_relaxed);
    atomic_store_explicit(&buffer->next, current, memory_order_relaxed);
  }
  while (!atomic_compare_exchange_weak_explicit(&list->stack, &current, next, memory_order_release, memory_order_relaxed));

  return current;
}

static inline struct InstantSharedBuffer* PopSharedBuffer(struct InstantSharedBufferList* list)
{
  uint64_t current;
  uint64_t next;
  uint32_t number;
  struct InstantSharedBuffer* buffer;

  do
  {
    current = atomic_load_explicit(&list->stack, memory_order_acquire);
    number  = (uint32_t)current;

    if (number == UINT32_MAX)
    {
      buffer = NULL;
      break;
    }

    buffer = list->data + number;
    next   = atomic_load_explicit(&buffer->next, memory_order_relaxed);
  }
  while (!atomic_compare_exchange_weak_explicit(&list->stack, &current, next, memory_order_acq_rel, memory_order_relaxed));

  return buffer;
}

static void InitializeSharedBufferList(struct InstantSharedBufferList* list)
{
  uint32_t number;
  struct InstantSharedBuffer* buffer;

  number = INSTANT_QUEUE_LENGTH;
  atomic_store_explicit(&list->stack, (uint64_t)UINT32_MAX, memory_order_relaxed);

  while ((number --) > 0)
  {
    buffer         = list->data + number;
    buffer->number = number;
    PushSharedBuffer(list, buffer);
  }
}

static struct InstantSharedBuffer* AllocateSharedBuffer(struct InstantSharedBufferList* list, int wait)
{
  struct InstantSharedBuffer* buffer;

  for ( ; ; )
  {
    if (buffer = PopSharedBuffer(list))
    {
      buffer->length = 0U;
      atomic_store_explicit(&buffer->count, 1, memory_order_relaxed);
      return buffer;
    }

    if ((wait == 0) ||
        (syscall(SYS_futex, WATCH(&list->stack), FUTEX_WAIT_BITSET | FUTEX_PRIVATE_FLAG, UINT32_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
        (errno != EINTR) &&
        (errno != EAGAIN))

    {
      //
      return NULL;
    }
  }
}

static void ReleaseSharedBuffer(struct InstantSharedBufferList* list, struct InstantSharedBuffer* buffer)
{
  if ((buffer != NULL) &&
      (atomic_fetch_sub_explicit(&buffer->count, 1, memory_order_relaxed) == 1) &&
      (PushSharedBuffer(list, buffer) == UINT32_MAX))
  {
    while ((syscall(SYS_futex, WATCH(&list->stack), FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, 1, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
           (errno == EINTR));
  }
}

static int AppendSendingQueue(struct InstantSendingQueue* queue, struct InstantSharedBuffer* buffer)
{
  uint32_t head;
  uint32_t tail;

  if ((queue  == NULL) ||
      (buffer == NULL))
  {
    //
    return -EINVAL;
  }

  do
  {
    head = atomic_load_explicit(&queue->head, memory_order_relaxed);
    tail = atomic_load_explicit(&queue->tail, memory_order_acquire);

    if ((uint32_t)(head - tail) >= INSTANT_QUEUE_LENGTH)
    {
      //
      return -EAGAIN;
    }
  }
  while (!atomic_compare_exchange_weak_explicit(&queue->head, &head, head + 1, memory_order_acq_rel, memory_order_relaxed));

  atomic_store_explicit(&queue->data[head & (INSTANT_QUEUE_LENGTH - 1)], buffer, memory_order_release);

  if (atomic_fetch_add_explicit(&queue->count, 1, memory_order_release) == 0)
  {
    while ((syscall(SYS_futex, (uint32_t*)&queue->count, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, 1, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
           (errno == EINTR));
  }

  return 0;
}

static struct InstantSharedBuffer* AdvanceSendingQueue(struct InstantSendingQueue* queue)
{
  uint32_t tail;
  uint32_t slot;
  struct InstantSharedBuffer* buffer;

  buffer = NULL;

  if (atomic_load_explicit(&queue->count, memory_order_acquire) != 0)
  {
    tail = atomic_load_explicit(&queue->tail, memory_order_relaxed);
    slot = tail & (INSTANT_QUEUE_LENGTH - 1);

    while ((buffer = atomic_load_explicit(&queue->data[slot], memory_order_acquire)) == NULL)
    {
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
      __asm__ __volatile__("yield");
#else
      __asm__ __volatile__("" ::: "memory");
#endif
    }

    atomic_store_explicit(&queue->data[slot], NULL, memory_order_release);
    atomic_store_explicit(&queue->tail, tail + 1, memory_order_release);
    atomic_fetch_sub_explicit(&queue->count, 1, memory_order_release);
  }

  return buffer;
}

static void AppendRequestQueue(struct InstantRequestQueue* queue, struct InstantRequestItem* item)
{
  struct InstantRequestItem* other;

  if (other = queue->tail)
  {
    other->next         = item;
    other->request.next = &item->request;
  }
  else
  {
    //
    queue->head = item;
  }

  item->next         = NULL;
  item->request.next = NULL;
  queue->tail        = item;
}

static void AdvanceRequestQueue(struct InstantRequestQueue* queue)
{
  struct InstantRequestItem* item;

  if (((item        = queue->head) != NULL) &&
      ((queue->head = item->next)  == NULL))
  {
    // Reset tail when queue is empty
    queue->tail = NULL;
  }
}

static struct InstantRequestItem* AllocateRequestItem(struct InstantReplicator* replicator)
{
  struct InstantRequestItem* item;

  if (item = replicator->items)
  {
    replicator->items = item->next;
    item->next        = NULL;
    memset(&item->element, 0, sizeof(struct ibv_sge));
    memset(&item->request, 0, sizeof(struct ibv_send_wr));
    return item;
  }

  return (struct InstantRequestItem*)calloc(1, sizeof(struct InstantRequestItem));
}

static int ReserveRequestItemList(struct InstantReplicator* replicator, uint32_t count)
{
  struct InstantRequestItem* item;

  item = replicator->items;

  while ((count > 0) &&
         (item != NULL))
  {
    item = item->next;
    count --;
  }

  while ((count > 0) &&
         (item = (struct InstantRequestItem*)calloc(1, sizeof(struct InstantRequestItem))))
  {
    item->next        = replicator->items;
    replicator->items = item;
    count --;
  }

  return -count;
}

static void ReleaseRequestItem(struct InstantReplicator* replicator, struct InstantRequestItem* item)
{
  item->next        = replicator->items;
  replicator->items = item;
}

static struct InstantTask* AllocateTask(struct InstantReplicator* replicator)
{
  struct InstantTask* task;

  if (task = replicator->tasks)
  {
    replicator->tasks = task->next;
    memset(task, 0, sizeof(struct InstantTask));
    return task;
  }

  return (struct InstantTask*)calloc(1, sizeof(struct InstantTask));
}

static void SubmitTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantTask* other;

  task->previous = replicator->schedule.tail;
  task->number   = replicator->schedule.number ++;

  if (other = task->previous)  other->next               = task;
  else                         replicator->schedule.head = task;

  replicator->schedule.count += task->type >= INSTANT_TASK_TYPE_READING;
  replicator->schedule.tail   = task;
}

static void RemoveTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantTask* other;

  if (task != NULL)
  {
    if (other = task->previous) other->next               = task->next;
    else                        replicator->schedule.head = task->next;

    if (other = task->next)     other->previous           = task->previous;
    else                        replicator->schedule.tail = task->previous;

    switch (task->type)
    {
      case INSTANT_TASK_TYPE_SYNCING:
        free(task->syncing.list);
        break;

      case INSTANT_TASK_TYPE_READING:
      case INSTANT_TASK_TYPE_WRITING:
        ReleaseSharedBuffer(&replicator->buffers, task->transfer.buffer);
        break;
    }

    task->previous              = NULL;
    task->next                  = replicator->tasks;
    replicator->tasks           = task;
    replicator->schedule.count -= task->type >= INSTANT_TASK_TYPE_READING;
  }
}

// Cookies

static struct InstantCookie* EnsureCookie(struct InstantReplicator* replicator, struct ReliablePool* pool)
{
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct InstantCookie* other;
  struct ReliableShare* share;
  struct InstantCard* card;
  struct ibv_mr* region;
  uint32_t number;

  pthread_mutex_lock(&replicator->lock);
  pthread_rwlock_rdlock(&pool->lock);

  cookie = (struct InstantCookie*)pool->closures[1];
  share  = (struct ReliableShare*)pool->closures[0];
  card   = replicator->cards;

  if ((cookie != NULL) &&
      (cookie->share != share))
  {
    if (other = replicator->cookies.tail)  other->next              = cookie;
    else                                   replicator->cookies.head = cookie;

    replicator->cookies.tail = cookie;
    cookie->expiration       = replicator->tick + COOKIE_EXPIRATION_COUNT;
    cookie                   = NULL;
    pool->closures[1]        = NULL;
  }

  if ((cookie == NULL) &&
      (cookie  = (struct InstantCookie*)calloc(1, sizeof(struct InstantCookie))))
  {
    memory              = share->memory;
    cookie->share       = share;
    cookie->data.length = sizeof(struct InstantCookieData);
    pool->closures[1]   = cookie;
    memcpy(cookie->data.name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);
    atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_WEAK, memory_order_relaxed);
  }

  pthread_rwlock_unlock(&pool->lock);

  if ((cookie != NULL) &&
      (card   != NULL) &&
      (cookie->data.count <= card->number))
  {
    number              = cookie->data.count;
    cookie->data.count  = card->number + 1;
    cookie->data.length = sizeof(struct InstantCookieData) + cookie->data.count * sizeof(uint32_t);

    do
    {
      if (region = ibv_reg_mr(card->domain, share->memory, share->size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC))
      {
        cookie->regions[card->number]   = region;
        cookie->data.keys[card->number] = region->rkey;
      }

      card = card->next;
    }
    while ((card != NULL) &&
           (card->number >= number));

  }

  pthread_mutex_unlock(&replicator->lock);

  return cookie;
}

static struct InstantCookie* FindCookie(struct InstantReplicator* replicator, const char* name)
{
  struct ReliablePool* pool;
  struct InstantCookie* cookie;
  struct ReliableIndexer* indexer;

  cookie = NULL;

  if ((indexer = replicator->indexer) &&
      (pool    = FindReliablePool(indexer, name, 1)))
  {
    cookie = EnsureCookie(replicator, pool);
    RetireReliablePool(pool);
  }

  return cookie;
}

static void RetireCookie(struct InstantReplicator* replicator, struct ReliablePool* pool)
{
  struct InstantCookie* cookie;
  struct InstantCookie* other;

  pthread_mutex_lock(&replicator->lock);
  pthread_rwlock_rdlock(&pool->lock);

  if (cookie = (struct InstantCookie*)pool->closures[1])
  {
    if (other = replicator->cookies.tail)  other->next              = cookie;
    else                                   replicator->cookies.head = cookie;

    replicator->cookies.tail = cookie;
    cookie->expiration       = replicator->tick + COOKIE_EXPIRATION_COUNT;
    pool->closures[1]        = NULL;
  }

  pthread_rwlock_unlock(&pool->lock);
  pthread_mutex_unlock(&replicator->lock);
}

static void DestroyCookie(struct InstantCookie* cookie)
{
  uint32_t number;
  struct ibv_mr* region;

  for (number = 0; number < cookie->data.count; ++ number)
  {
    if (region = cookie->regions[number])
    {
      // ibv_dereg_mr() is not NULL-tolerant
      ibv_dereg_mr(region);
    }
  }

  RetireReliableShare(cookie->share);
  free(cookie);
}

static int HandleCookieIterator(void* key, size_t size, void* data, void* argument1, void* argument2)
{
  EnsureCookie((struct InstantReplicator*)argument1, (struct ReliablePool*)data);
  return 0;
}

static void UpdateCookieList(struct InstantReplicator* replicator)
{
  struct ReliableIndexer* indexer;

  if (indexer = replicator->indexer)
  {
    pthread_rwlock_rdlock(&indexer->lock);
    IterateThroughHashMap(indexer->map, HandleCookieIterator, replicator, NULL);
    pthread_rwlock_unlock(&indexer->lock);
  }
}

static void TrackCookieList(struct InstantReplicator* replicator)
{
  struct InstantCookie* cookie;

  pthread_mutex_lock(&replicator->lock);

  while ((cookie = replicator->cookies.head) &&
         (replicator->tick > cookie->expiration))
  {
    replicator->cookies.head = cookie->next;
    DestroyCookie(cookie);
  }

  if (replicator->cookies.head == NULL)
  {
    // Queue is empty
    replicator->cookies.tail = NULL;
  }

  pthread_mutex_unlock(&replicator->lock);
}

// Removals

static void CreateRemoval(struct InstantReplicator* replicator, struct InstantRemovalData* data)
{
  struct InstantRemoval* removal;
  struct InstantRemoval* other;

  if (removal = replicator->removals.stack)
  {
    replicator->removals.stack = removal->next;
    removal->next              = NULL;
  }

  if ((removal != NULL) ||
      (removal  = (struct InstantRemoval*)calloc(1, sizeof(struct InstantRemoval))))
  {
    memcpy(&removal->data, data, sizeof(struct InstantRemovalData));

    if (other = replicator->removals.tail)  other->next               = removal;
    else                                    replicator->removals.head = removal;

    removal->expiration       = replicator->tick + REMOVAL_EXPIRATION_COUNT;
    replicator->removals.tail = removal;
  }
}

static void ApplyRemoval(struct InstantReplicator* replicator, struct InstantRemoval* removal)
{
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  uint32_t number;

  pool   = NULL;
  cookie = NULL;
  number = FindReliableBlockNumber(replicator->indexer, removal->data.name, removal->data.identifier);

  if ((number != UINT32_MAX) &&
      (pool    = FindReliablePool(replicator->indexer, removal->data.name, 1)) &&
      (cookie  = EnsureCookie(replicator, pool)))
  {
    share  = cookie->share;
    memory = share->memory;
    block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

    if (((uintptr_t)block < ((uintptr_t)share->memory + share->size)) &&
        (atomic_load_explicit(&block->count, memory_order_acquire) > 0))
    {
      // Block is busy, it is up to application what to do
      CallReliableMonitor(RELIABLE_MONITOR_BLOCK_REMOVAL, pool, share, block);
    }
    else
    {
      // Try to remove block silently
      FreeReliableBlock(pool, number, removal->data.identifier);
    }
  }

  RetireReliablePool(pool);
}

static void TrackRemovalList(struct InstantReplicator* replicator)
{
  struct InstantRemoval* removal;

  pthread_mutex_lock(&replicator->lock);

  while ((removal = replicator->removals.head) &&
         (replicator->tick > removal->expiration))
  {
    replicator->removals.head  = removal->next;
    removal->next              = replicator->removals.stack;
    replicator->removals.stack = removal;

    ApplyRemoval(replicator, removal);
  }

  if (replicator->removals.head == NULL)
  {
    // Queue is empty
    replicator->removals.tail = NULL;
  }

  pthread_mutex_unlock(&replicator->lock);
}

// Submissions

static int SubmitSharedBuffer(struct InstantReplicator* replicator, struct InstantPeer* peer, struct InstantSharedBuffer* buffer)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;
  struct ibv_mr* region;

  if ((peer->state == INSTANT_PEER_STATE_CONNECTED) &&
      (card = peer->card)                           &&
      (item = AllocateRequestItem(replicator)))
  {
    region                   = card->region2;
    item->element.addr       = (uintptr_t)buffer->data;
    item->element.length     = buffer->length;
    item->element.lkey       = region->lkey;
    item->request.wr_id      = (uintptr_t)buffer;
    item->request.sg_list    = &item->element;
    item->request.num_sge    = 1;
    item->request.opcode     = IBV_WR_SEND_WITH_IMM;
    item->request.send_flags = IBV_SEND_SIGNALED;
    item->request.imm_data   = card->number;

    atomic_fetch_add_explicit(&buffer->count, 1, memory_order_relaxed);
    AppendRequestQueue(&peer->queue, item);
    return 0;
  }

  return -1;
}

static struct InstantRequestItem* SubmitWritingWork(struct InstantReplicator* replicator, struct InstantPeer* peer, uint32_t key, struct InstantBlockData* entry, struct ibv_mr* region, struct ReliableBlock* block)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;

  // Use ReserveRequestItemList() first to make guaranteed allocation
  // Here is two-stage WRITE work to make grant CAS-based consistency of block on remote

  card = peer->card;
  item = AllocateRequestItem(replicator);

  item->element.addr                = (uintptr_t)&block->mark + sizeof(uint64_t);
  item->element.length              = sizeof(struct ReliableBlock) - offsetof(struct ReliableBlock, mark) - sizeof(uint64_t) + block->length;
  item->element.lkey                = region->lkey;
  item->request.sg_list             = &item->element;
  item->request.num_sge             = 1;
  item->request.opcode              = IBV_WR_RDMA_WRITE;
  item->request.wr.rdma.remote_addr = entry->address + sizeof(uint64_t);
  item->request.wr.rdma.rkey        = key;

  AppendRequestQueue(&peer->queue, item);
  item = AllocateRequestItem(replicator);

  item->element.addr                = (uintptr_t)&block->mark;
  item->element.length              = sizeof(uint64_t);
  item->element.lkey                = region->lkey;
  item->request.sg_list             = &item->element;
  item->request.num_sge             = 1;
  item->request.opcode              = IBV_WR_RDMA_WRITE;
  item->request.wr.rdma.remote_addr = entry->address;
  item->request.wr.rdma.rkey        = key;

  AppendRequestQueue(&peer->queue, item);
  return item;
}

static int UpdateWritingWork(struct InstantRequestItem* item, struct InstantTask* task, uint32_t number)
{
  if (item != NULL)
  {
    item->request.wr_id      = (uintptr_t)task;
    item->request.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
    item->request.send_flags = IBV_SEND_SIGNALED;
    item->request.imm_data   = number;
    return 0;
  }

  return -1;
}

static struct InstantRequestItem* SubmitExchangingWork(struct InstantReplicator* replicator, struct InstantPeer* peer, uint32_t key, struct InstantBlockData* entry, uint64_t* result)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;
  struct ibv_mr* region;

  // Use ReserveRequestItemList() first to make guaranteed allocation

  card = peer->card;
  item = AllocateRequestItem(replicator);

  region                              = card->region2;
  item->element.addr                  = (uintptr_t)result;
  item->element.length                = sizeof(uint64_t);
  item->element.lkey                  = region->lkey;
  item->request.sg_list               = &item->element;
  item->request.num_sge               = 1;
  item->request.opcode                = IBV_WR_ATOMIC_CMP_AND_SWP;
  item->request.wr.atomic.remote_addr = entry->address;
  item->request.wr.atomic.rkey        = key;
  item->request.wr.atomic.compare_add = entry->mark;
  item->request.wr.atomic.swap        = entry->mark | 1ULL;

  AppendRequestQueue(&peer->queue, item);
  return item;
}

static int UpdateExchangingWork(struct InstantRequestItem* item, struct InstantTask* task)
{
  if (item != NULL)
  {
    item->request.wr_id      = (uintptr_t)task;
    item->request.send_flags = IBV_SEND_SIGNALED;
    return 0;
  }

  return -1;
}

// Monitor

static void HandleBlockChange(struct InstantReplicator* replicator, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block)
{
  static __thread struct InstantSharedBuffer* buffer = NULL;
  static __thread struct InstantCookie* cookie = NULL;
  static __thread struct ReliablePool* last = NULL;

  struct InstantHeaderData* header;
  struct InstantBlockData* entry;
  uintptr_t address;

  if (block != NULL)
  {
    if (last != pool)
    {
      AppendSendingQueue(&replicator->queue, buffer);
      buffer = NULL;
      cookie = NULL;
      last   = pool;
    }

    if ((buffer == NULL) &&
        (cookie  = EnsureCookie(replicator, pool)) &&
        (buffer  = AllocateSharedBuffer(&replicator->buffers, 1)))
    {
      pthread_mutex_lock(&replicator->lock);
      header         = (struct InstantHeaderData*)buffer->data;
      header->type   = INSTANT_TYPE_NOTIFY;
      header->task   = UINT32_MAX;
      buffer->length = sizeof(struct InstantHeaderData) + cookie->data.length;
      memcpy(buffer->data + sizeof(struct InstantHeaderData), &cookie->data, cookie->data.length);
      uuid_copy(header->identifier, replicator->identifier);
      pthread_mutex_unlock(&replicator->lock);
    }

    if (buffer != NULL)
    {
      // Most likely, passed block belongs to shadow share, that requires to remap its virtual address to actual cooked share
      address  = (uintptr_t)&block->mark - (uintptr_t)share->memory;
      share    = cookie->share;
      address += (uintptr_t)share->memory;

      entry           = (struct InstantBlockData*)(buffer->data + buffer->length);
      entry->mark     = atomic_load_explicit(&block->mark, memory_order_acquire);
      entry->hint     = atomic_load_explicit(&block->hint, memory_order_relaxed);
      entry->length   = sizeof(struct ReliableBlock) - offsetof(struct ReliableBlock, mark) + block->length;
      entry->address  = address;
      buffer->length += sizeof(struct InstantBlockData);
      uuid_copy(entry->identifier, block->identifier);
    }
  }

  if ((buffer != NULL) &&
      ((block == NULL) ||
       (buffer->length >= INSTANT_MESSAGE_LENGTH_THRESHOLD)))
  {
    AppendSendingQueue(&replicator->queue, buffer);
    buffer = NULL;
    cookie = NULL;
    last   = NULL;
  }
}

static void HandleBlockRelease(struct InstantReplicator* replicator, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block)
{
  struct InstantSharedBuffer* buffer;
  struct InstantRemovalData* removal;
  struct InstantHeaderData* header;
  struct ReliableMemory* memory;

  if (buffer = AllocateSharedBuffer(&replicator->buffers, 1))
  {
    memory         = share->memory;
    header         = (struct InstantHeaderData*)buffer->data;
    header->type   = INSTANT_TYPE_REMOVE;
    header->task   = UINT32_MAX;
    buffer->length = sizeof(struct InstantHeaderData) + sizeof(struct InstantRemovalData);
    removal        = (struct InstantRemovalData*)(buffer->data + sizeof(struct InstantHeaderData));

    uuid_copy(header->identifier,  replicator->identifier);
    uuid_copy(removal->identifier, block->identifier);
    memcpy(removal->name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);

    AppendSendingQueue(&replicator->queue, buffer);
  }
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct InstantReplicator* replicator;

  replicator = (struct InstantReplicator*)closure;

  switch (event)
  {
    case RELIABLE_MONITOR_POOL_CREATE:
      replicator->indexer = GetReliableIndexer(pool);

    case RELIABLE_MONITOR_SHARE_CREATE:
      EnsureCookie(replicator, pool);
      break;

    case RELIABLE_MONITOR_POOL_RELEASE:
      RetireCookie(replicator, pool);
      break;

    case RELIABLE_MONITOR_BLOCK_CHANGE:
    case RELIABLE_MONITOR_FLUSH_COMMIT:
      HandleBlockChange(replicator, pool, share, block);
      break;

    case RELIABLE_MONITOR_BLOCK_RELEASE:
      HandleBlockRelease(replicator, pool, share, block);
      break;
  }
}

static void HandleSendingQueue(struct InstantReplicator* replicator, int result)
{
  struct InstantSharedBuffer* buffer;
  struct io_uring_sqe* submission;
  struct InstantPeer* peer;

  while (buffer = AdvanceSendingQueue(&replicator->queue))
  {
    pthread_mutex_lock(&replicator->lock);

    for (peer = replicator->peers; peer != NULL; peer = peer->next)
    {
      // SubmitSharedBuffer() increments buffer->count
      SubmitSharedBuffer(replicator, peer, buffer);
    }

    pthread_mutex_unlock(&replicator->lock);
    ReleaseSharedBuffer(&replicator->buffers, buffer);
  }

  if (submission = io_uring_get_sqe(&replicator->ring))
  {
    io_uring_prep_futex_wait(submission, (uint32_t*)&replicator->queue.count, 0, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
    io_uring_sqe_set_data64(submission, RING_TAG_SENDING_QUEUE);
  }
}

// Message and work handling

static void CreateClockingTask(struct InstantReplicator* replicator, struct InstantPeer* peer)
{
  struct InstantTask* task;

  for (task = replicator->schedule.head; task != NULL; task = task->next)
  {
    if (task->type == INSTANT_TASK_TYPE_CLOCKING)
    {
      // Change the clocking scope to broadcast
      task->peer = NULL;
      return;
    }
  }

  if (task = AllocateTask(replicator))
  {
    task->type = INSTANT_TASK_TYPE_CLOCKING;
    task->peer = peer;

    SubmitTask(replicator, task);
  }
}

static int ExecuteClockingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantSharedBuffer* buffer;
  struct InstantHeaderData* header;
  struct InstantPeer* peer;
  struct timespec* time;

  if (!(buffer = AllocateSharedBuffer(&replicator->buffers, 0)))
  {
    task->state = INSTANT_TASK_STATE_WAIT_BUFFER;
    return 0;
  }

  header         = (struct InstantHeaderData*)buffer->data;
  header->type   = INSTANT_TYPE_CLOCK;
  header->task   = UINT32_MAX;
  buffer->length = sizeof(struct InstantHeaderData) + sizeof(struct timespec);
  time           = (struct timespec*)(buffer->data + sizeof(struct InstantHeaderData));

  uuid_copy(header->identifier, replicator->identifier);
  clock_gettime(CLOCK_REALTIME, time);

  if (task->peer != NULL)
  {
    SubmitSharedBuffer(replicator, task->peer, buffer);
    ReleaseSharedBuffer(&replicator->buffers, buffer);
    return -1;
  }

  pthread_mutex_lock(&replicator->lock);

  for (peer = replicator->peers; peer != NULL; peer = peer->next)
  {
    // SubmitSharedBuffer() increments buffer->count
    SubmitSharedBuffer(replicator, peer, buffer);
  }

  pthread_mutex_unlock(&replicator->lock);
  ReleaseSharedBuffer(&replicator->buffers, buffer);
  return -1;
}

static int HandleSyncingIterator(void* key, size_t size, void* data, void* argument1, void* argument2)
{
  struct InstantReplicator* replicator;
  struct InstantTask* task;

  replicator = (struct InstantReplicator*)argument1;

  if (task = AllocateTask(replicator))
  {
    memcpy(task->name, key, RELIABLE_MEMORY_NAME_LENGTH);

    task->type         = INSTANT_TASK_TYPE_SYNCING;
    task->peer         = (struct InstantPeer*)argument2;
    task->syncing.list = CollectReliableBlockList(replicator->indexer, (struct ReliablePool*)data, NULL, RELIABLE_COLLECT_IN_USE);

    SubmitTask(replicator, task);
  }

  return 0;
}

static void CreateSyncingTask(struct InstantReplicator* replicator, struct InstantPeer* peer)
{
  struct ReliableIndexer* indexer;

  if (indexer = replicator->indexer)
  {
    pthread_rwlock_rdlock(&indexer->lock);
    IterateThroughHashMap(indexer->map, HandleSyncingIterator, replicator, peer);
    pthread_rwlock_unlock(&indexer->lock);
  }
}

static int ExecuteSyncingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantSharedBuffer* buffer;
  struct InstantHeaderData* header;
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  uint32_t number;
  uintptr_t limit;

  if ((task->syncing.list == NULL)                             ||
      (task->syncing.list[task->syncing.cursor] == UINT32_MAX) ||
      !(cookie = FindCookie(replicator, task->name)))
  {
    // Empty list, removed pool or sending complete
    return -1;
  }

  if (!(buffer = AllocateSharedBuffer(&replicator->buffers, 0)))
  {
    task->state = INSTANT_TASK_STATE_WAIT_BUFFER;
    return 0;
  }

  share  = cookie->share;
  memory = share->memory;
  limit  = (uintptr_t)share->memory + share->size;

  header         = (struct InstantHeaderData*)buffer->data;
  header->type   = INSTANT_TYPE_NOTIFY;
  header->task   = UINT32_MAX;
  buffer->length = sizeof(struct InstantHeaderData) + cookie->data.length;

  memcpy(buffer->data + sizeof(struct InstantHeaderData), &cookie->data, cookie->data.length);
  uuid_copy(header->identifier, replicator->identifier);

  while ((task->syncing.list[task->syncing.cursor] != UINT32_MAX) &&
         (buffer->length < INSTANT_MESSAGE_LENGTH_THRESHOLD))
  {
    number = task->syncing.list[task->syncing.cursor ++];
    block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

    if ((limit > ((uintptr_t)block)) &&
        (limit > ((uintptr_t)block + sizeof(struct ReliableBlock) + (uintptr_t)block->length)) &&
        (atomic_load_explicit(&block->count, memory_order_acquire) > 0) &&
        (~atomic_load_explicit(&block->mark, memory_order_relaxed) & 1ULL))
    {
      entry           = (struct InstantBlockData*)(buffer->data + buffer->length);
      entry->mark     = atomic_load_explicit(&block->mark, memory_order_acquire);
      entry->hint     = atomic_load_explicit(&block->hint, memory_order_relaxed);
      entry->length   = sizeof(struct ReliableBlock) - offsetof(struct ReliableBlock, mark) + block->length;
      entry->address  = (uintptr_t)&block->mark;
      buffer->length += sizeof(struct InstantBlockData);
      uuid_copy(entry->identifier, block->identifier);
    }
  }

  if (buffer->length != (sizeof(struct InstantHeaderData) + cookie->data.length))
  {
    // Avoid sending empty messages
    SubmitSharedBuffer(replicator, task->peer, buffer);
  }

  ReleaseSharedBuffer(&replicator->buffers, buffer);

  task->state = INSTANT_TASK_STATE_PROGRESS;
  return 0;
}

static int TransmitRetrieveRequest(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantSharedBuffer* buffer;
  struct InstantHeaderData* header;
  struct InstantCookie* cookie;

  if (!(cookie = FindCookie(replicator, task->name)))
  {
    // Not really sure how that possible, but, well...
    return -1;
  }

  if ( (ReserveRequestItemList(replicator, 1) < 0) ||
      !(buffer = AllocateSharedBuffer(&replicator->buffers, 0)))
  {
    task->state = INSTANT_TASK_STATE_WAIT_BUFFER;
    return 0;
  }

  header         = (struct InstantHeaderData*)buffer->data;
  header->type   = INSTANT_TYPE_RETRIEVE;
  header->task   = task->number;
  buffer->length = sizeof(struct InstantHeaderData) + cookie->data.length;

  uuid_copy(header->identifier, replicator->identifier);
  memcpy(buffer->data + sizeof(struct InstantHeaderData), &cookie->data, cookie->data.length);
  memcpy(buffer->data + buffer->length, task->transfer.entries, task->transfer.count * sizeof(struct InstantBlockData));

  buffer->length        += task->transfer.count * sizeof(struct InstantBlockData);
  task->transfer.buffer  = buffer;
  task->state            = INSTANT_TASK_STATE_WAIT_DATA;

  return SubmitSharedBuffer(replicator, task->peer, buffer);
}

static struct InstantBlockData* FindStaleInstantBlockData(uuid_t identifier, uint64_t hint, struct InstantBlockData* cursor, uint32_t count)
{
  while (count != 0)
  {
    if ((uuid_compare(cursor->identifier, identifier) == 0) &&
        (cursor->hint >= hint))
    {
      // Suggest removing only an outdated entry
      return cursor;
    }

    cursor ++;
    count  --;
  }

  return NULL;
}

static void PruneStaleReadingTask(struct InstantReplicator* replicator, const char* name, struct InstantBlockData* entries, uint32_t count)
{
  struct InstantBlockData* entry;
  struct InstantTask* task;
  struct InstantTask* next;
  uint32_t index;

  for (task = replicator->schedule.head; task != NULL; task = next)
  {
    next = task->next;

    if ((task->type  == INSTANT_TASK_TYPE_READING) &&
        (task->state == INSTANT_TASK_STATE_IDLE)   &&
        (memcmp(task->name, name, RELIABLE_MEMORY_NAME_LENGTH) == 0))
    {
      for (index = 0; index < task->transfer.count; )
      {
        entry = task->transfer.entries + index;

        if (FindStaleInstantBlockData(entry->identifier, entry->hint, entries, count) != NULL)
        {
          task->transfer.count --;
          memmove(entry, entry + 1, (task->transfer.count - index) * sizeof(struct InstantBlockData));
          continue;
        }

        index ++;
      }

      if (task->transfer.count == 0)
      {
        RemoveTask(replicator, task);
        continue;
      }
    }
  }
}

static int CollectReadingBlockList(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  struct InstantPeer* peer;
  uint32_t number;
  uint32_t index;
  uint64_t mark;
  uint64_t hint;

  peer = task->peer;

  if (peer->state != INSTANT_PEER_STATE_CONNECTED)
  {
    // Connection lost
    return -1;
  }

  if (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_READY)
  {
    task->state = INSTANT_TASK_STATE_WAIT_LOCK;
    return 0;
  }

  if (!(pool   = FindReliablePool(replicator->indexer, task->name, 1)) ||
      !(cookie = EnsureCookie(replicator, pool)))
  {
    RetireReliablePool(pool);
    return -1;
  }

  for (index = 0; index < task->transfer.count; )
  {
    entry = task->transfer.entries + index;

    if (((number = FindReliableBlockNumber(replicator->indexer, task->name, entry->identifier))   == UINT32_MAX) &&
        (((number = ReserveReliableBlock(pool, entry->identifier, RELIABLE_TYPE_NON_RECOVERABLE)) == UINT32_MAX) ||
         ((cookie = EnsureCookie(replicator, pool))                                               == NULL)))
    {
      RetireReliablePool(pool);
      return -1;
    }

    share  = cookie->share;
    memory = share->memory;
    block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
    mark   = atomic_load_explicit(&block->mark, memory_order_relaxed);
    hint   = atomic_load_explicit(&block->hint, memory_order_relaxed);

    if ((mark & 1ULL) ||
        (hint >= entry->hint))
    {
      task->transfer.count --;
      memmove(entry, entry + 1, (task->transfer.count - index) * sizeof(struct InstantBlockData));
      continue;
    }

    atomic_store_explicit(&block->hint, entry->hint, memory_order_relaxed);
    task->transfer.numbers[index] = number;
    index ++;
  }

  RetireReliablePool(pool);

  if (task->transfer.count == 0)
  {
    // List of entries is empty now, removing the task
    return -1;
  }

  task->state = INSTANT_TASK_STATE_PROGRESS;
  return 1;
}

static int PrepareReadingBlockList(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  uint32_t* number;
  uint32_t index;
  uint64_t mark;
  uint64_t hint;

  if (!(cookie = FindCookie(replicator, task->name)))
  {
    // Cannot acquire cookie
    return -1;
  }

  share  = cookie->share;
  memory = share->memory;

  for (index = 0; index < task->transfer.count; )
  {
    number = task->transfer.numbers + index;
    entry  = task->transfer.entries + index;
    block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)*number);
    mark   = atomic_load_explicit(&block->mark, memory_order_relaxed);
    hint   = atomic_load_explicit(&block->hint, memory_order_relaxed);

    if ((mark & 1ULL) ||
        (hint > entry->hint))
    {
      task->transfer.count --;
      memmove(entry,  entry  + 1, (task->transfer.count - index) * sizeof(struct InstantBlockData));
      memmove(number, number + 1, (task->transfer.count - index) * sizeof(uint32_t));
      continue;
    }

    entry->hint    = hint;
    entry->mark    = mark;
    entry->address = (uintptr_t)&block->mark;
    index ++;
  }

  if (task->transfer.count == 0)
  {
    // List of entries is empty now, removing the task
    return -1;
  }

  return TransmitRetrieveRequest(replicator, task);
}

static int ExecuteReadingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  switch (task->state)
  {
    case INSTANT_TASK_STATE_IDLE:
    case INSTANT_TASK_STATE_WAIT_LOCK:
      return CollectReadingBlockList(replicator, task);

    case INSTANT_TASK_STATE_PROGRESS:
      return PrepareReadingBlockList(replicator, task);

    case INSTANT_TASK_STATE_WAIT_BUFFER:
      return TransmitRetrieveRequest(replicator, task);

    case INSTANT_TASK_STATE_WAIT_DATA:
    case INSTANT_TASK_STATE_WAIT_COMPLETION:
      return 0;
  }

  return -1;
}

static void TransmitTaskComplete(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantSharedBuffer* buffer;
  struct InstantHeaderData* header;

  if (buffer = task->transfer.buffer)
  {
    header         = (struct InstantHeaderData*)buffer->data;
    header->type   = INSTANT_TYPE_COMPLETE;
    header->task   = task->transfer.task;
    buffer->length = sizeof(struct InstantHeaderData);

    uuid_copy(header->identifier, replicator->identifier);

    SubmitSharedBuffer(replicator, task->peer, buffer);
  }
}

static int PrepareWritingBlockList(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantSharedBuffer* buffer;
  struct InstantRequestItem* item;
  struct InstantBlockData* entry;
  struct InstantPeer* peer;
  uint32_t number;
  uint32_t index;

  if (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_READY)
  {
    task->state = INSTANT_TASK_STATE_WAIT_LOCK;
    return 0;
  }

  if (!(task->transfer.buffer = AllocateSharedBuffer(&replicator->buffers, 0)))
  {
    task->state = INSTANT_TASK_STATE_WAIT_BUFFER;
    return 0;
  }

  if ((FindCookie(replicator, task->name) == NULL) ||
      (ReserveRequestItemList(replicator, task->transfer.count) < 0))
  {
    TransmitTaskComplete(replicator, task);
    return -1;
  }

  buffer = task->transfer.buffer;
  peer   = task->peer;
  item   = NULL;

  for (index = 0; index < task->transfer.count; )
  {
    entry  = task->transfer.entries + index;
    number = FindReliableBlockNumber(replicator->indexer, task->name, entry->identifier);

    if (number == UINT32_MAX)
    {
      task->transfer.count --;
      memmove(entry, entry + 1, (task->transfer.count - index) * sizeof(struct InstantBlockData));
      continue;
    }

    task->transfer.numbers[index] = number;
    buffer->values[index]         = 0ULL;

    item   = SubmitExchangingWork(replicator, peer, task->transfer.key, entry, buffer->values + index);
    index ++;
  }

  if (UpdateExchangingWork(item, task) < 0)
  {
    TransmitTaskComplete(replicator, task);
    return -1;
  }

  task->state         = INSTANT_TASK_STATE_WAIT_COMPLETION;
  task->transfer.code = IBV_WC_COMP_SWAP;
  return 0;
}

static void ResetReadingBlockList(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  uint32_t number;
  uint32_t index;
  uint64_t mark;

  if (cookie = FindCookie(replicator, task->name))
  {
    share  = cookie->share;
    memory = share->memory;

    for (index = 0; index < task->transfer.count; ++ index)
    {
      number = task->transfer.numbers[index];
      entry  = task->transfer.entries + index;
      block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
      mark   = entry->mark | 1ULL;
      atomic_compare_exchange_strong_explicit(&block->mark, &mark, entry->mark, memory_order_relaxed, memory_order_relaxed);
    }
  }
}

static int ExecuteWritingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  switch (task->state)
  {
    case INSTANT_TASK_STATE_IDLE:
    case INSTANT_TASK_STATE_WAIT_LOCK:
    case INSTANT_TASK_STATE_WAIT_BUFFER:
      return PrepareWritingBlockList(replicator, task);

    case INSTANT_TASK_STATE_WAIT_DATA:
    case INSTANT_TASK_STATE_WAIT_COMPLETION:
      return 0;
  }

  return -1;
}

static void CreateTransferTask(struct InstantReplicator* replicator, struct InstantPeer* peer, struct InstantHeaderData* header, uint32_t length, uint32_t number)
{
  struct InstantCookieData* cookie;
  struct InstantBlockData* entries;
  struct InstantTask* task;
  intptr_t count;
  uint32_t index;

  cookie  = (struct InstantCookieData*)((uint8_t*)header + sizeof(struct InstantHeaderData));
  entries = (struct InstantBlockData*)((uint8_t*)header + sizeof(struct InstantHeaderData) + cookie->length);
  count   = (uintptr_t)header + length - (uintptr_t)entries;

  if ((count  > 0LL)           &&
      (number < cookie->count) &&
      (task = AllocateTask(replicator)))
  {
    task->peer           = peer;
    task->type           = header->type;
    task->transfer.task  = header->task;
    task->transfer.key   = cookie->keys[number];
    task->transfer.count = count / sizeof(struct InstantBlockData);

    memcpy(task->name, cookie->name, RELIABLE_MEMORY_NAME_LENGTH);
    memcpy(task->transfer.entries, entries, count);

    for (index = 0; index < task->transfer.count; index ++)
    {
      // Adjust hint to local epoch
      task->transfer.entries[index].hint += peer->vector;
    }

    if (task->type == INSTANT_TASK_TYPE_READING)
    {
      // Prune outdated entries and tasks
      PruneStaleReadingTask(replicator, task->name, task->transfer.entries, task->transfer.count);
    }

    SubmitTask(replicator, task);
  }
}

static void ApplyClock(struct InstantReplicator* replicator, struct InstantPeer* peer, struct timespec* time)
{
  peer->vector = GetReliableTrackerClockVector(time);
}

static void HandleReceivedMessage(struct InstantReplicator* replicator, uint8_t* data, uint32_t length, uint32_t number, int status, int flags)
{
  struct InstantHeaderData* header;
  struct InstantPeer* peer;
  struct InstantTask* task;

  if ((status == IBV_WC_SUCCESS) &&
      (length >= sizeof(struct InstantHeaderData)))
  {
    header = (struct InstantHeaderData*)data;

    pthread_mutex_lock(&replicator->lock);
    for (peer = replicator->peers; (peer != NULL) && (uuid_compare(peer->identifier, header->identifier) != 0); peer = peer->next);
    pthread_mutex_unlock(&replicator->lock);

    if (peer == NULL)
    {
      // Not really sure how that possible, but, well...
      return;
    }

    if (peer->state == INSTANT_PEER_STATE_CONNECTING)
    {
      // The message can be received before or after CM events
      peer->state = INSTANT_PEER_STATE_CONNECTED;
    }

    switch (header->type)
    {
      case INSTANT_TYPE_USER:
        CallEventFunction(replicator, INSTANT_REPLICATOR_EVENT_USER_MESSAGE, peer, data + sizeof(struct InstantHeaderData), length - sizeof(struct InstantHeaderData));
        break;

      case INSTANT_TYPE_CLOCK:
        if (length == (sizeof(struct InstantHeaderData) + sizeof(struct timespec)))
          ApplyClock(replicator, peer, (struct timespec*)(data + sizeof(struct InstantHeaderData)));
        break;

      case INSTANT_TYPE_REMOVE:
        if (length == (sizeof(struct InstantHeaderData) + sizeof(struct InstantRemovalData)))
          CreateRemoval(replicator, (struct InstantRemovalData*)(data + sizeof(struct InstantHeaderData)));
        break;

      case INSTANT_TYPE_COMPLETE:
        for (task = replicator->schedule.head; (task != NULL) && (task->number != header->task); task = task->next);
        RemoveTask(replicator, task);
        break;

      case INSTANT_TYPE_NOTIFY:
      case INSTANT_TYPE_RETRIEVE:
        if ((flags & IBV_WC_WITH_IMM) &&
            (length >= (sizeof(struct InstantHeaderData) + sizeof(struct InstantCookieData))))
          CreateTransferTask(replicator, peer, header, length, number);
        break;
    }
  }
}

static void HandleTranferredData(struct InstantReplicator* replicator, uint32_t identifier, int status)
{
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  struct InstantPeer* peer;
  struct InstantTask* task;
  uint32_t* number;
  uint32_t count;
  uint32_t index;
  uint64_t mark;

  pool   = NULL;
  cookie = NULL;

  for (task = replicator->schedule.head; (task != NULL) && (task->number != identifier); task = task->next);

  if ((task   != NULL)           &&
      (status == IBV_WC_SUCCESS) &&
      (pool    = FindReliablePool(replicator->indexer, task->name, 1)) &&
      (cookie  = EnsureCookie(replicator, pool)))
  {
    peer   = task->peer;
    share  = cookie->share;
    memory = share->memory;
    count  = 0;

    for (index = 0; index < task->transfer.count; ++ index)
    {
      number = task->transfer.numbers + index;
      entry  = task->transfer.entries + index;
      block  = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)*number);
      mark   = atomic_load_explicit(&block->mark, memory_order_relaxed);

      if (mark != entry->mark)
      {
        if ((block->length <= memory->size - offsetof(struct ReliableBlock, data)) &&
            (GetCRC32C(block->data, block->length, 0) == atomic_load_explicit(&block->control, memory_order_relaxed)))
        {
          // Assumption: valid CRC here means the block was updated by this RETRIEVE flow.
          // If another writer updates the same block concurrently, hint normalization can be wrong.
          atomic_fetch_add_explicit(&block->hint, peer->vector, memory_order_relaxed);
          CallReliableMonitor(RELIABLE_MONITOR_BLOCK_ARRIVAL, pool, share, block);
          *number = UINT32_MAX;
          continue;
        }

        if (task->transfer.attempt >= READING_ATTEMPT_COUNT)
        {
          CallReliableMonitor(RELIABLE_MONITOR_BLOCK_DAMAGE, pool, share, block);
          *number = UINT32_MAX;
          continue;
        }

        count ++;
        continue;
      }

      *number = UINT32_MAX;
    }

    RetireReliablePool(pool);

    if (count != 0)
    {
      for (index = 0; index < task->transfer.count; )
      {
        number = task->transfer.numbers + index;
        entry  = task->transfer.entries + index;

        if (*number == UINT32_MAX)
        {
          task->transfer.count --;
          memmove(entry,  entry  + 1, (task->transfer.count - index) * sizeof(struct InstantBlockData));
          memmove(number, number + 1, (task->transfer.count - index) * sizeof(uint32_t));
          continue;
        }

        index ++;
      }

      ReleaseSharedBuffer(&replicator->buffers, task->transfer.buffer);
      task->state             = INSTANT_TASK_STATE_WAIT_BUFFER;
      task->transfer.buffer   = NULL;
      task->transfer.attempt ++;
      return;
    }
  }

  RemoveTask(replicator, task);
}

static void HandleCompletedExchange(struct InstantReplicator* replicator, struct InstantTask* task, int status)
{
  struct InstantSharedBuffer* buffer;
  struct InstantRequestItem* item;
  struct InstantBlockData* entry;
  struct ReliableMemory* memory;
  struct InstantCookie* cookie;
  struct ReliableBlock* block;
  struct ReliableShare* share;
  struct ReliablePool* pool;
  struct InstantPeer* peer;
  struct InstantCard* card;
  struct ibv_mr* region;
  uint32_t number;
  uint32_t index;

  if (task != NULL)
  {
    peer = task->peer;
    pool = NULL;
    item = NULL;

    if ((status == IBV_WC_SUCCESS)                                                &&
        (ReserveRequestItemList(replicator, 2 * INSTANT_BATCH_LENGTH_LIMIT) == 0) &&
        (pool    = FindReliablePool(replicator->indexer, task->name, 1))          &&
        (cookie  = EnsureCookie(replicator, pool)))
    {
      card   = peer->card;
      region = cookie->regions[card->number];
      share  = cookie->share;
      memory = share->memory;
      buffer = task->transfer.buffer;

      for (index = 0; index < task->transfer.count; ++ index)
      {
        number = task->transfer.numbers[index];
        entry  = task->transfer.entries + index;

        if (buffer->values[index] == entry->mark)
        {
          block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
          item  = SubmitWritingWork(replicator, peer, task->transfer.key, entry, region, block);
        }
      }
    }

    if (UpdateWritingWork(item, task, task->transfer.task) < 0)
    {
      TransmitTaskComplete(replicator, task);
      RemoveTask(replicator, task);
    }

    RetireReliablePool(pool);

    task->state         = INSTANT_TASK_STATE_WAIT_COMPLETION;
    task->transfer.code = IBV_WC_RDMA_WRITE;
  }
}

static void HandleCompletedWrite(struct InstantReplicator* replicator, struct InstantTask* task, int status)
{
  // Status makes no sense for writing task completion
  RemoveTask(replicator, task);
}

static void WaitForReadyState(struct InstantReplicator* replicator)
{
  struct io_uring_sqe* submission;

  if ((~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_READY) &&
      (submission = io_uring_get_sqe(&replicator->ring)))
  {
    atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_LOCK, memory_order_relaxed);
    io_uring_prep_futex_wait(submission, (uint32_t*)&replicator->state, INSTANT_REPLICATOR_STATE_ACTIVE | INSTANT_REPLICATOR_STATE_LOCK, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
    io_uring_sqe_set_data64(submission, RING_TAG_READY_STATE);
  }
}

static void ExecuteTaskList(struct InstantReplicator* replicator)
{
  static ExecuteInstantTaskFunction functions[] =
  {
    ExecuteSyncingTask,
    ExecuteClockingTask,
    ExecuteReadingTask,
    ExecuteWritingTask
  };

  int result;
  int condition;
  struct InstantTask* task;
  struct InstantTask* next;
  ExecuteInstantTaskFunction function;

  do
  {
    condition = 0;

    for (task = replicator->schedule.head; task != NULL; task = next)
    {
      next       = task->next;
      function   = functions[task->type];
      result     = function(replicator, task);
      condition |= (result > 0);

      if (result < 0)
      {
        // Negative result means remove immediately
        RemoveTask(replicator, task);
      }
    }
  }
  while (condition != 0);

  if ((replicator->schedule.count != 0) &&
      (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_LOCK))
  {
    atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_LOCK, memory_order_relaxed);
    CallEventFunction(replicator, INSTANT_REPLICATOR_EVENT_FLUSH, NULL, NULL, 0);
    while ((syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, INT_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
           (errno == EINTR));
    WaitForReadyState(replicator);
  }

  if ((replicator->schedule.count == 0) &&
      (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_LOCK))
  {
    atomic_fetch_and_explicit(&replicator->state, ~(INSTANT_REPLICATOR_STATE_LOCK | INSTANT_REPLICATOR_STATE_READY), memory_order_relaxed);
    while ((syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, INT_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
           (errno == EINTR));
  }
}

static void ClearTaskList(struct InstantReplicator* replicator, struct InstantPeer* peer)
{
  struct InstantTask* task;
  struct InstantTask* next;

  for (task = replicator->schedule.head; task != NULL; task = next)
  {
    next = task->next;

    if (task->peer == peer)
    {
      if (task->state == INSTANT_TASK_STATE_WAIT_DATA)
      {
        // The reading blocks can be blocked by remote CAS
        ResetReadingBlockList(replicator, task);
      }

      if (task->state < INSTANT_TASK_STATE_WAIT_COMPLETION)
      {
        // Tasks in state INSTANT_TASK_STATE_WAIT_COMPLETION should be removed by HandleCompletedRead() / HandleCompletedWrite()
        RemoveTask(replicator, task);
      }
    }
  }
}

// Transport abstractions

static int SubmitReceivingBuffer(struct InstantCard* card, uint64_t number)
{
  struct ibv_mr* region;
  struct ibv_sge element;
  struct ibv_recv_wr request;
  struct ibv_recv_wr* temporary;

  region          = card->region1;
  element.addr    = (uintptr_t)card->buffers[number];
  element.length  = INSTANT_BUFFER_LENGTH;
  element.lkey    = region->lkey;
  request.wr_id   = (uintptr_t)card->buffers[number];
  request.next    = NULL;
  request.sg_list = &element;
  request.num_sge = 1;

  return ibv_post_srq_recv(card->queue1, &request, &temporary);
}

static int ReplaceReceivingBuffer(struct InstantCard* card, uint64_t work)
{
  uint64_t number;

  number  = (work - (uintptr_t)card->buffers) / INSTANT_BUFFER_LENGTH;
  number ^= 1ULL;

  return SubmitReceivingBuffer(card, number);
}

static int SubmitReceivingBufferList(struct InstantCard* card)
{
  uint64_t number;

  for (number = 0; (number < INSTANT_QUEUE_LENGTH * 2) && (SubmitReceivingBuffer(card, number) == 0); number += 2);

  return -(number != INSTANT_QUEUE_LENGTH * 2);
}

static struct InstantCard* EnsureCard(struct InstantReplicator* replicator, struct ibv_context* context)
{
  struct InstantCard* card;
  struct InstantCard* other;
  struct io_uring_sqe* submission;
  struct ibv_srq_init_attr attribute;

  for (card = replicator->cards; (card != NULL) && (card->context != context); card = card->next);

  if ((card == NULL) &&
      (!(other = replicator->cards) ||
        (other->number < (INSTANT_CARD_COUNT - 1))) &&
      (card = (struct InstantCard*)calloc(1, sizeof(struct InstantCard))))
  {
    memset(&attribute, 0, sizeof(struct ibv_srq_init_attr));

    attribute.attr.max_wr  = INSTANT_QUEUE_LENGTH;
    attribute.attr.max_sge = 1;

    if (!((card->channel = ibv_create_comp_channel(context))         &&
          (card->domain  = ibv_alloc_pd(context))                    &&
          (card->queue1  = ibv_create_srq(card->domain, &attribute)) &&
          (card->queue2  = ibv_create_cq(context, INSTANT_QUEUE_LENGTH * 2, NULL, card->channel, 0)) &&
          (card->region1 = ibv_reg_mr(card->domain, card->buffers,        sizeof(card->buffers),       IBV_ACCESS_LOCAL_WRITE)) &&
          (card->region2 = ibv_reg_mr(card->domain, &replicator->buffers, sizeof(replicator->buffers), IBV_ACCESS_LOCAL_WRITE)) &&
          (SubmitReceivingBufferList(card) == 0)))
    {
      if (card->queue1   != NULL)  ibv_destroy_srq(card->queue1);
      if (card->queue2   != NULL)  ibv_destroy_cq(card->queue2);
      if (card->region1  != NULL)  ibv_dereg_mr(card->region1);
      if (card->region2  != NULL)  ibv_dereg_mr(card->region2);
      if (card->domain   != NULL)  ibv_dealloc_pd(card->domain);
      if (card->channel  != NULL)  ibv_destroy_comp_channel(card->channel);

      free(card);
      return NULL;
    }

    card->attribute.cap.max_send_wr     = INSTANT_QUEUE_LENGTH;
    card->attribute.cap.max_recv_wr     = INSTANT_QUEUE_LENGTH;
    card->attribute.cap.max_send_sge    = 1;
    card->attribute.cap.max_recv_sge    = attribute.attr.max_sge;
    card->attribute.cap.max_inline_data = 256;
    card->attribute.sq_sig_all          = 0;
    card->attribute.srq                 = card->queue1;
    card->attribute.send_cq             = card->queue2;
    card->attribute.recv_cq             = card->queue2;
    card->attribute.qp_type             = IBV_QPT_RC;
    card->context                       = context;

    ibv_req_notify_cq(card->queue2, 0);

    if (submission = io_uring_get_sqe(&replicator->ring))
    {
      io_uring_prep_poll_add(submission, card->channel->fd, POLLIN);
      io_uring_sqe_set_data(submission, card);
    }

    pthread_mutex_lock(&replicator->lock);

    if (other = replicator->cards)
    {
      card->next      = other;
      card->number    = other->number + 1;
      other->previous = card;
    }

    replicator->cards = card;

    pthread_mutex_unlock(&replicator->lock);

    UpdateCookieList(replicator);
  }

  return card;
}

static void DestroyCard(struct InstantCard* card)
{
  ibv_destroy_srq(card->queue1);
  ibv_destroy_cq(card->queue2);
  ibv_dereg_mr(card->region1);
  ibv_dereg_mr(card->region2);
  ibv_dealloc_pd(card->domain);
  ibv_destroy_comp_channel(card->channel);
  free(card);
}

static int EnsureWorkOperationCode(struct InstantReplicator* replicator, struct InstantCard* card, struct ibv_wc* completion)
{
  struct InstantTask* task;

  // “If the completion status is other than IBV_WC_SUCCESS, only … wr_id, status, qp_num, vendor_err.”
  // https://man.archlinux.org/man/ibv_poll_cq.3.en

  if (completion->status == IBV_WC_SUCCESS)
    return completion->opcode;

  if ((completion->wr_id >= ((uintptr_t)card->buffers)) &&
      (completion->wr_id <  ((uintptr_t)card->buffers + INSTANT_QUEUE_LENGTH * 2 * INSTANT_BUFFER_LENGTH)))
    return IBV_WC_RECV;

  if ((completion->wr_id >= ((uintptr_t)&replicator->buffers)) &&
      (completion->wr_id <  ((uintptr_t)&replicator->buffers) + sizeof(struct InstantSharedBufferList)))
    return IBV_WC_SEND;

  if (task = (struct InstantTask*)completion->wr_id)
    return task->transfer.code;

  return -1;
}

static void HandleCompletionChannel(struct InstantReplicator* replicator, struct InstantCard* card, int result)
{
  void* context;
  struct ibv_cq* queue;
  struct ibv_wc* completion;
  struct ibv_wc completions[32];
  struct io_uring_sqe* submission;

  if ((result > 0)      &&
      (result & POLLIN) &&
      (ibv_get_cq_event(card->channel, &queue, &context) == 0))
  {
    ibv_ack_cq_events(queue, 1);
    ibv_req_notify_cq(queue, 0);

    while ((result = ibv_poll_cq(queue, COUNT(completions), completions)) > 0)
    {
      for (completion = completions; completion < (completions + result); ++ completion)
      {
        switch (EnsureWorkOperationCode(replicator, card, completion))
        {
          case IBV_WC_SEND:
            ReleaseSharedBuffer(&replicator->buffers, (struct InstantSharedBuffer*)completion->wr_id);
            break;

          case IBV_WC_RECV:
            ReplaceReceivingBuffer(card, completion->wr_id);
            HandleReceivedMessage(replicator, (uint8_t*)completion->wr_id, completion->byte_len, completion->imm_data, completion->status, completion->wc_flags & IBV_WC_WITH_IMM);
            break;

          case IBV_WC_RECV_RDMA_WITH_IMM:
            ReplaceReceivingBuffer(card, completion->wr_id);
            HandleTranferredData(replicator, completion->imm_data, completion->status);
            break;

          case IBV_WC_RDMA_WRITE:
            HandleCompletedWrite(replicator, (struct InstantTask*)completion->wr_id, completion->status);
            break;

          case IBV_WC_COMP_SWAP:
            HandleCompletedExchange(replicator, (struct InstantTask*)completion->wr_id, completion->status);
            break;
        }
      }
    }
  }

  if (submission = io_uring_get_sqe(&replicator->ring))
  {
    io_uring_prep_poll_add(submission, card->channel->fd, POLLIN);
    io_uring_sqe_set_data(submission, card);
  }
}

static void ClearRequestQueue(struct InstantReplicator* replicator, struct InstantRequestQueue* queue)
{
  struct InstantRequestItem* item;

  while (item = queue->head)
  {
    if (item->request.send_flags & IBV_SEND_SIGNALED)
    {
      switch (item->request.opcode)
      {
        case IBV_WR_SEND:
        case IBV_WR_SEND_WITH_IMM:
          ReleaseSharedBuffer(&replicator->buffers, (struct InstantSharedBuffer*)item->request.wr_id);
          break;

        case IBV_WR_RDMA_WRITE:
        case IBV_WR_RDMA_WRITE_WITH_IMM:
          HandleCompletedWrite(replicator, (struct InstantTask*)item->request.wr_id, IBV_WC_WR_FLUSH_ERR);
          break;

        case IBV_WR_ATOMIC_CMP_AND_SWP:
          HandleCompletedExchange(replicator, (struct InstantTask*)item->request.wr_id, IBV_WC_WR_FLUSH_ERR);
          break;
      }
    }

    AdvanceRequestQueue(queue);
    ReleaseRequestItem(replicator, item);
  }
}

static void DrainRequestQueueList(struct InstantReplicator* replicator)
{
  int result;
  struct InstantPeer* peer;
  struct ibv_send_wr* request;
  struct rdma_cm_id* descriptor;
  struct InstantRequestItem* item;

  pthread_mutex_lock(&replicator->lock);

  for (peer = replicator->peers; peer != NULL; peer = peer->next)
  {
    if ((peer->state == INSTANT_PEER_STATE_CONNECTED) &&
        (descriptor   = peer->descriptor))
    {
      if (item = peer->queue.head)
      {
        request = NULL;
        result  = ibv_post_send(descriptor->qp, &item->request, &request);

        while ((item = peer->queue.head) &&
               ((result  == 0) ||
                (request != &item->request)))
        {
          AdvanceRequestQueue(&peer->queue);
          ReleaseRequestItem(replicator, item);
        }
      }
    }
  }

  pthread_mutex_unlock(&replicator->lock);
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
  struct InstantCard* card;
  struct InstantPeer* peer;
  struct InstantHandshakeData* handshake;
  uint8_t digest[SHA_DIGEST_LENGTH];

  card      = EnsureCard(replicator, descriptor->verbs);
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

  peer = (struct InstantPeer*)descriptor->context;

  peer->state      = INSTANT_PEER_STATE_CONNECTED;
  peer->fails      = 0;
  peer->points[peer->round].rank = 0;

  CreateClockingTask(replicator, peer);
  CreateSyncingTask(replicator, peer);

  CallEventFunction(replicator, INSTANT_REPLICATOR_EVENT_CONNECTED, peer, NULL, 0);

  return 0;
}

static int HandleDisconnected(struct InstantReplicator* replicator, struct rdma_cm_id* descriptor, int reason)
{
  struct InstantPeer* peer;

  if ((peer = (struct InstantPeer*)descriptor->context) &&
      (peer->descriptor == descriptor))
  {
    if (peer->state == INSTANT_PEER_STATE_CONNECTED)
    {
      CallEventFunction(replicator, INSTANT_REPLICATOR_EVENT_DISCONNECTED, peer, NULL, reason);

      ClearTaskList(replicator, peer);
      ClearRequestQueue(replicator, &peer->queue);
    }

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
  if (rdma_resolve_route(descriptor, GENERIC_POLL_TIMEOUT) != 0)
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

  card       = EnsureCard(replicator, descriptor->verbs);
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

static void HandleEventChannel(struct InstantReplicator* replicator, int result)
{
  struct io_uring_sqe* submission;
  struct rdma_cm_id* descriptor;
  struct rdma_cm_event* event;

  if ((result > 0)      &&
      (result & POLLIN) &&
      (rdma_get_cm_event(replicator->channel, &event) == 0))
  {
    result     = 0;
    descriptor = event->id;

    switch (event->event)
    {
      case RDMA_CM_EVENT_CONNECT_REQUEST:
        result = HandleConnectRequest(replicator, descriptor, &event->param.conn);
        break;

      case RDMA_CM_EVENT_ADDR_RESOLVED:
        result = HandleAddressResolved(replicator, descriptor);
        break;

      case RDMA_CM_EVENT_ROUTE_RESOLVED:
        result = HandleRouteResolved(replicator, descriptor);
        break;

      case RDMA_CM_EVENT_ESTABLISHED:
        result = HandleEstablished(replicator, descriptor);
        break;

      case RDMA_CM_EVENT_ADDR_ERROR:
      case RDMA_CM_EVENT_ROUTE_ERROR:
      case RDMA_CM_EVENT_CONNECT_ERROR:
      case RDMA_CM_EVENT_UNREACHABLE:
      case RDMA_CM_EVENT_REJECTED:
      case RDMA_CM_EVENT_DISCONNECTED:
        result = HandleDisconnected(replicator, descriptor, event->event);
        break;

      case RDMA_CM_EVENT_DEVICE_REMOVAL:
        raise(SIGABRT);
    }

    rdma_ack_cm_event(event);
    DestroyDescriptor(descriptor, result);
  }

  if (submission = io_uring_get_sqe(&replicator->ring))
  {
    io_uring_prep_poll_add(submission, replicator->channel->fd, POLLIN);
    io_uring_sqe_set_data64(submission, RING_TAG_EVENT_CHANNEL);
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

  if ((rdma_create_id(replicator->channel, &descriptor, peer, RDMA_PS_TCP)                          != 0) ||
      (rdma_resolve_addr(descriptor, NULL, (struct sockaddr*)&point->address, GENERIC_POLL_TIMEOUT) != 0))
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
      if (peer->fails >= CONNECTION_ATTEMPT_COUNT)
      {
        if (next     != NULL)  next->previous    = previous;
        if (previous != NULL)  previous->next    = next;
        else                   replicator->peers = next;

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
  struct __kernel_timespec interval;
  struct io_uring_sqe* submission;
  struct io_uring_cqe* completion;
  unsigned head;
  int result;

  replicator       = (struct InstantReplicator*)closure;
  interval.tv_nsec = GENERIC_POLL_TIMEOUT * 1000000ULL;
  interval.tv_sec  = 0;

  pthread_setname_np(replicator->thread, "Replicator");

  if (submission = io_uring_get_sqe(&replicator->ring))
  {
    io_uring_prep_timeout(submission, &interval, 0, IORING_TIMEOUT_MULTISHOT);
    io_uring_sqe_set_data64(submission, RING_TAG_TIMEOUT);
  }

  HandleSendingQueue(replicator, 0);
  HandleEventChannel(replicator, 0);

  while (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
  {
    result = io_uring_submit_and_wait_timeout(&replicator->ring, &completion, 1, &interval, NULL);

    if ((result == -EINVAL) ||
        (result == -ENOMEM) ||
        (result == -EFAULT))
    {
      atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_FAILURE, memory_order_relaxed);
      break;
    }

    io_uring_for_each_cqe(&replicator->ring, head, completion)
    {
      switch (completion->user_data)
      {
        case 0ULL:
        case LIBURING_UDATA_TIMEOUT:
          break;

        case RING_TAG_TIMEOUT:
          replicator->tick ++;
          TrackPeerList(replicator);
          TrackCookieList(replicator);
          TrackRemovalList(replicator);
          CreateClockingTask(replicator, NULL);
          break;

        case RING_TAG_READY_STATE:
          WaitForReadyState(replicator);
          break;

        case RING_TAG_SENDING_QUEUE:
          HandleSendingQueue(replicator, completion->res);
          break;

        case RING_TAG_EVENT_CHANNEL:
          HandleEventChannel(replicator, completion->res);
          break;

        default:
          HandleCompletionChannel(replicator, (struct InstantCard*)completion->user_data, completion->res);
          break;
      }

      io_uring_cq_advance(&replicator->ring, 1);
    }

    ExecuteTaskList(replicator);
    DrainRequestQueueList(replicator);
  }

  atomic_fetch_and_explicit(&replicator->state, ~(INSTANT_REPLICATOR_STATE_LOCK | INSTANT_REPLICATOR_STATE_READY), memory_order_relaxed);

  while ((syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, INT_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
         (errno == EINTR));

  return NULL;
}

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, HandleInstantEventFunction function, void* closure, struct ReliableMonitor* next)
{
  struct InstantReplicator* replicator;
  struct rdma_addrinfo* information;
  pthread_mutexattr_t attribute;
  struct rdma_addrinfo hint;
  char service[16];

  if ((replicator = (struct InstantReplicator*)calloc(1, sizeof(struct InstantReplicator))) &&
      (io_uring_queue_init(INSTANT_QUEUE_LENGTH, &replicator->ring, IORING_SETUP_SUBMIT_ALL | IORING_SETUP_COOP_TASKRUN) == 0))
  {
    io_uring_ring_dontfork(&replicator->ring);
    io_uring_set_iowait(&replicator->ring, 0);

    pthread_mutexattr_init(&attribute);
    pthread_mutexattr_settype(&attribute, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&replicator->lock, &attribute);
    pthread_mutexattr_destroy(&attribute);

    replicator->super.next     = next;
    replicator->super.closure  = replicator;
    replicator->super.function = HandleMonitorEvent;
    replicator->function       = function;
    replicator->closure        = closure;
    replicator->secret         = strdup(secret);
    replicator->name           = strdup(name);

    if (identifier == NULL)  uuid_generate(replicator->identifier);
    else                     uuid_copy(replicator->identifier, identifier);

    replicator->handshake.magic = INSTANT_MAGIC;

    getrandom((uint8_t*)&replicator->handshake.nonce, sizeof(uint16_t), 0);
    strncpy(replicator->handshake.name, name, INSTANT_SERVICE_NAME_LENGTH);
    uuid_copy(replicator->handshake.identifier, replicator->identifier);
    HMAC(EVP_sha1(), secret, strlen(secret), (uint8_t*)&replicator->handshake, offsetof(struct InstantHandshakeData, digest), replicator->handshake.digest, NULL);

    InitializeSharedBufferList(&replicator->buffers);

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
  struct InstantTask* task;
  struct InstantCookie* cookie;
  struct InstantRemoval* removal;
  struct InstantRequestItem* item;

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
      ClearRequestQueue(replicator, &peer->queue);
      DestroyDescriptor(peer->descriptor, 1);
      free(peer);
    }

    while (task = replicator->schedule.head)
    {
      // Remove all pending tasks that left
      RemoveTask(replicator, task);
    }

    while (task = replicator->tasks)
    {
      replicator->tasks = task->next;
      free(task);
    }

    while (removal = replicator->removals.head)
    {
      replicator->removals.head = removal->next;
      free(removal);
    }

    while (removal = replicator->removals.stack)
    {
      replicator->removals.stack = removal->next;
      free(removal);
    }

    while (cookie = replicator->cookies.head)
    {
      replicator->cookies.head = cookie->next;
      DestroyCookie(cookie);
    }

    while (card = replicator->cards)
    {
      replicator->cards = card->next;
      DestroyCard(card);
    }

    while (item = replicator->items)
    {
      replicator->items = item->next;
      free(item);
    }

    if (replicator->descriptor != NULL)  rdma_destroy_ep(replicator->descriptor);
    if (replicator->channel    != NULL)  rdma_destroy_event_channel(replicator->channel);

    pthread_mutex_destroy(&replicator->lock);
    io_uring_queue_exit(&replicator->ring);
    free(replicator->secret);
    free(replicator->name);
    free(replicator);
  }
}

int FlushInstantReplicator(struct InstantReplicator* replicator)
{
  uint32_t state;

  if (replicator != NULL)
  {
    state = atomic_load_explicit(&replicator->state, memory_order_acquire);

    if (state & INSTANT_REPLICATOR_STATE_LOCK)
    {
      atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_READY, memory_order_release);

      while ((syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, INT_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
             (errno == EINTR));

      state = INSTANT_REPLICATOR_STATE_ACTIVE | INSTANT_REPLICATOR_STATE_LOCK | INSTANT_REPLICATOR_STATE_READY;

      while ((atomic_load_explicit(&replicator->state, memory_order_relaxed) == state) &&
             (syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAIT_BITSET | FUTEX_PRIVATE_FLAG, state, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
             ((errno == EINTR) ||
              (errno == EAGAIN)));

      state = atomic_load_explicit(&replicator->state, memory_order_relaxed);
    }

    if ((~state & INSTANT_REPLICATOR_STATE_ACTIVE) ||
        ( state & INSTANT_REPLICATOR_STATE_FAILURE))
    {
      // Indicate a fault
      return -EFAULT;
    }

    return 0;
  }

  return -EINVAL;
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

int TransmitInstantReplicatorUserMessage(struct InstantReplicator* replicator, const char* data, uint32_t length, int wait)
{
  struct InstantSharedBuffer* buffer;
  struct InstantHeaderData* header;

  if ((replicator == NULL) ||
      (data       == NULL) &&
      (length != 0)        ||
      (length >= (INSTANT_BUFFER_LENGTH - sizeof(struct InstantHeaderData))))
  {
    //
    return -EINVAL;
  }

  if (buffer = AllocateSharedBuffer(&replicator->buffers, wait))
  {
    header         = (struct InstantHeaderData*)buffer->data;
    header->type   = INSTANT_TYPE_USER;
    header->task   = UINT32_MAX;
    buffer->length = sizeof(struct InstantHeaderData) + length;

    uuid_copy(header->identifier,  replicator->identifier);
    memcpy(buffer->data + sizeof(struct InstantHeaderData), data, length);

    AppendSendingQueue(&replicator->queue, buffer);
    return 0;
  }

  return -EBUSY;
}
