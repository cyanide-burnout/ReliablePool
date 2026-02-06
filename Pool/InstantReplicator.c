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

#define RING_TAG_TIMEOUT        (LIBURING_UDATA_TIMEOUT - 1ULL)
#define RING_TAG_READY_STATE    (LIBURING_UDATA_TIMEOUT - 2ULL)
#define RING_TAG_EVENT_CHANNEL  (LIBURING_UDATA_TIMEOUT - 3ULL)
#define RING_TAG_SENDING_QUEUE  (LIBURING_UDATA_TIMEOUT - 4ULL)

#define GENERIC_POLL_TIMEOUT       200
#define COOKIE_EXPIRATION_COUNT    (60000 / GENERIC_POLL_TIMEOUT)
#define CONNECTION_ATTEMPT_COUNT   128

#define COUNT(array)  (sizeof(array) / sizeof(array[0]))

#ifndef IORING_ENTER_NO_IOWAIT
#define io_uring_set_iowait(ring, value)
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN
#define WATCH(address)  ((uint32_t*)(address))
#else
#define WATCH(address)  ((uint32_t*)(address) + 1)
#endif

_Static_assert(sizeof(struct InstantHandshakeData) <= 56,                                                       "private_data_len for RDMA_PS_TCP must be maximum 56 bytes in length");
_Static_assert((sizeof(struct InstantSharedBuffer)             % sizeof(uint64_t)) == 0,                        "Size of InstantSharedBuffer must be 64-bit aligned");
_Static_assert((offsetof(struct InstantSharedBuffer, values)   % sizeof(uint64_t)) == 0,                        "InstantSharedBuffer.values must be 64-bit aligned");
_Static_assert((offsetof(struct InstantSharedBufferList, data) % sizeof(uint64_t)) == 0,                        "InstantSharedBufferList.data must be 64-bit aligned");
_Static_assert((offsetof(struct InstantReplicator, buffers)    % sizeof(uint64_t)) == 0,                        "InstantReplicator.buffers must be 64-bit aligned");
_Static_assert((INSTANT_QUEUE_LENGTH   != 0) && ((INSTANT_QUEUE_LENGTH   & (INSTANT_QUEUE_LENGTH   - 1)) == 0), "INSTANT_QUEUE_LENGTH must be a power of two");
_Static_assert((INSTANT_TASK_ALIGNMENT != 0) && ((INSTANT_TASK_ALIGNMENT & (INSTANT_TASK_ALIGNMENT - 1)) == 0), "INSTANT_TASK_ALIGNMENT must be a power of two");
_Static_assert(INSTANT_TASK_ALIGNMENT >= INSTANT_BATCH_LENGTH_LIMIT,                                            "INSTANT_TASK_ALIGNMENT must be equal or greater INSTANT_BATCH_LENGTH_LIMIT");

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
    memset(&item->element, 0, sizeof(struct ibv_sge));
    memset(&item->request, 0, sizeof(struct ibv_send_wr));
    return item;
  }

  return (struct InstantRequestItem*)calloc(1, sizeof(struct InstantRequestItem));
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

  if (posix_memalign((void**)&task, INSTANT_TASK_ALIGNMENT, sizeof(struct InstantTask)) == 0)
  {
    memset(task, 0, sizeof(struct InstantTask));
    return task;
  }

  return NULL;
}

static void SubmitTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantTask* other;

  task->previous = replicator->schedule.tail;
  task->number   = replicator->schedule.number ++;

  if (other = task->previous)  other->next               = task;
  else                         replicator->schedule.head = task; 

  replicator->schedule.count += task->type > INSTANT_TASK_TYPE_SYNCING;
  replicator->schedule.tail   = task;
}

static void RemoveTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  struct InstantTask* other;

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
  replicator->schedule.count -= task->type > INSTANT_TASK_TYPE_SYNCING;
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
  share  = pool->share;
  card   = replicator->cards;

  if ((cookie != NULL) &&
      (cookie->share != share))
  {
    if (other = replicator->cookies)
    {
      cookie->next    = other;
      other->previous = cookie;
    }

    replicator->cookies = cookie;
    cookie->expiration  = COOKIE_EXPIRATION_COUNT;
    cookie              = NULL;
    pool->closures[1]   = NULL;
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
      if (region = ibv_reg_mr(card->domain, share->memory, share->size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ))
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
    if (other = replicator->cookies)
    {
      cookie->next    = other;
      other->previous = cookie;
    }

    replicator->cookies = cookie;
    cookie->expiration  = COOKIE_EXPIRATION_COUNT;
    pool->closures[1]   = NULL;
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
  struct InstantCookie* next;
  struct InstantCookie* previous;

  pthread_mutex_lock(&replicator->lock);

  for (previous = NULL, cookie = replicator->cookies; cookie != NULL; cookie = next)
  {
    next = cookie->next;

    if ((-- cookie->expiration) == 0)
    {
      if (next     != NULL)  next->previous      = previous;
      if (previous != NULL)  previous->next      = next;
      else                   replicator->cookies = next;

      DestroyCookie(cookie);
      continue;
    }

    previous = cookie;
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

static int SubmitReadingWork(struct InstantReplicator* replicator, struct InstantPeer* peer, struct InstantTask* task, uint32_t key, struct InstantBlockData* entry, struct ibv_mr* region, struct ReliableBlock* block)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;

  if ((peer->state == INSTANT_PEER_STATE_CONNECTED) &&
      (card = peer->card)                           &&
      (item = AllocateRequestItem(replicator)))
  {
    item->element.addr                = (uintptr_t)&block->mark;
    item->element.length              = entry->length;
    item->element.lkey                = region->lkey;
    item->request.sg_list             = &item->element;
    item->request.num_sge             = 1;
    item->request.opcode              = IBV_WR_RDMA_READ;
    item->request.wr.rdma.remote_addr = entry->address;
    item->request.wr.rdma.rkey        = key;

    if (task != NULL)
    {
      item->request.wr_id      = (uintptr_t)task;
      item->request.send_flags = IBV_SEND_SIGNALED;
    }

    AppendRequestQueue(&peer->queue, item);
    return 0;
  }

  return -1;
}

static int SubmitWritingWork(struct InstantReplicator* replicator, struct InstantPeer* peer, struct InstantTask* task, uint32_t number, uint32_t key, struct InstantBlockData* entry, struct ibv_mr* region, struct ReliableBlock* block)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;

  if ((peer->state == INSTANT_PEER_STATE_CONNECTED) &&
      (card = peer->card)                           &&
      (item = AllocateRequestItem(replicator)))
  {
    item->element.addr                = (uintptr_t)&block->mark;
    item->element.length              = sizeof(struct ReliableBlock) - offsetof(struct ReliableBlock, mark) + block->length;
    item->element.lkey                = region->lkey;
    item->request.sg_list             = &item->element;
    item->request.num_sge             = 1;
    item->request.opcode              = IBV_WR_RDMA_WRITE;
    item->request.wr.rdma.remote_addr = entry->address;
    item->request.wr.rdma.rkey        = key;

    if (task != NULL)
    {
      item->request.wr_id      = (uintptr_t)task;
      item->request.opcode     = IBV_WR_RDMA_WRITE_WITH_IMM;
      item->request.send_flags = IBV_SEND_SIGNALED;
      item->request.imm_data   = number;
    }

    AppendRequestQueue(&peer->queue, item);
    return 0;
  }

  return -1;
}

static int SubmitSwappingWork(struct InstantReplicator* replicator, struct InstantPeer* peer, struct InstantTask* task, uint32_t number, uint32_t key, struct InstantBlockData* entry, struct ibv_mr* region, uint64_t* result)
{
  struct InstantRequestItem* item;
  struct InstantCard* card;

  if ((peer->state == INSTANT_PEER_STATE_CONNECTED) &&
      (card = peer->card)                           &&
      (item = AllocateRequestItem(replicator)))
  {
    item->element.addr                  = (uintptr_t)result;
    item->element.length                = sizeof(uint64_t);
    item->element.lkey                  = region->lkey;
    item->request.wr_id                 = (uintptr_t)task | (uintptr_t)number;
    item->request.sg_list               = &item->element;
    item->request.num_sge               = 1;
    item->request.opcode                = IBV_WR_ATOMIC_CMP_AND_SWP;
    item->request.send_flags            = IBV_SEND_SIGNALED;
    item->request.wr.atomic.remote_addr = entry->address;
    item->request.wr.atomic.rkey        = key;
    item->request.wr.atomic.compare_add = entry->mark;
    item->request.wr.atomic.swap        = entry->mark | 1ULL;

    AppendRequestQueue(&peer->queue, item);
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
    // Emplty lost, removed pool or sending complete
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

static int ExecuteReadingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  return -1;
}

static int ExecuteWritingTask(struct InstantReplicator* replicator, struct InstantTask* task)
{
  return -1;
}

static void HandleReceivedMessage(struct InstantReplicator* replicator, uint8_t* data, uint32_t length, uint32_t number, int status, int flags)
{
  struct InstantHeaderData* header;
  struct InstantCookieData* cookie;
  struct InstantBlockData* list;
  struct InstantPeer* peer;
  struct InstantTask* task;
  intptr_t count;

  if ((status == IBV_WC_SUCCESS) &&
      (length >= sizeof(struct InstantHeaderData)))
  {
    header = (struct InstantHeaderData*)data;

    pthread_mutex_lock(&replicator->lock);
    for (peer = replicator->peers; (peer != NULL) && (uuid_compare(peer->identifier, header->identifier) != 0); peer = peer->next);
    pthread_mutex_unlock(&replicator->lock);

    if ((peer != NULL) &&
        (peer->state == INSTANT_PEER_STATE_CONNECTING))
    {
      // The message can be received before or after CM events
      peer->state = INSTANT_PEER_STATE_CONNECTED;
    }

    if ((peer != NULL) &&
        (flags & IBV_WC_WITH_IMM) &&
        (header->type >= INSTANT_TYPE_NOTIFY) &&
        (header->type <= INSTANT_TYPE_RETREIVE) &&
        (length >= (sizeof(struct InstantHeaderData) + sizeof(struct InstantCookieData))))
    {
      cookie = (struct InstantCookieData*)(data + sizeof(struct InstantHeaderData));
      list   = (struct InstantBlockData*)(data + sizeof(struct InstantHeaderData) + cookie->length);
      count  = (uintptr_t)data + length - (uintptr_t)list;

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
        memcpy(task->transfer.list, list, count);

        SubmitTask(replicator, task);
      }
    }
  }
}

static void HandleTranferredData(struct InstantReplicator* replicator, uint32_t number, int status)
{
  struct InstantTask* task;

  for (task = replicator->schedule.head; (task != NULL) && (task->number != number); task = task->next);

  if (task != NULL)
  {

    RemoveTask(replicator, task);
  }
}

static void HandleCompletedRead(struct InstantReplicator* replicator, struct InstantTask* task, int status)
{
  // if (status != IBV_WC_SUCCESS)
  RemoveTask(replicator, task);
}

static void HandleCompletedSwap(struct InstantReplicator* replicator, struct InstantTask* task, uint16_t number, int status)
{
  // if (status != IBV_WC_SUCCESS)
  RemoveTask(replicator, task);
}

static void HandleCompletedWrite(struct InstantReplicator* replicator, struct InstantTask* task, int status)
{
  // if (status != IBV_WC_SUCCESS)
  RemoveTask(replicator, task);
}

static void WaitForReadyState(struct InstantReplicator* replicator)
{
  struct io_uring_sqe* submission;

  if ((~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_READY) &&
      (submission = io_uring_get_sqe(&replicator->ring)))
  {
    atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_HOLD, memory_order_relaxed);
    io_uring_prep_futex_wait(submission, (uint32_t*)&replicator->state, INSTANT_REPLICATOR_STATE_ACTIVE | INSTANT_REPLICATOR_STATE_HOLD, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
    io_uring_sqe_set_data64(submission, RING_TAG_READY_STATE);
  }
}

static void ExecuteTaskList(struct InstantReplicator* replicator)
{
  static ExecuteInstantTaskFunction functions[] =
  {
    ExecuteSyncingTask,
    ExecuteReadingTask,
    ExecuteWritingTask
  };

  struct InstantTask* task;
  struct InstantTask* next;
  ExecuteInstantTaskFunction function;

  for (task = replicator->schedule.head; task != NULL; task = next)
  {
    next     = task->next;
    function = functions[task->type];

    if (function(replicator, task) < 0)
    {
      // Negative result means remove immediately
      RemoveTask(replicator, task);
    }
  }

  if ((replicator->schedule.count != 0) &&
      (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_HOLD))
  {
    atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_HOLD, memory_order_relaxed);
    WaitForReadyState(replicator);
  }

  if ((replicator->schedule.count == 0) &&
      (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_HOLD))
  {
    atomic_fetch_and_explicit(&replicator->state, ~(INSTANT_REPLICATOR_STATE_HOLD | INSTANT_REPLICATOR_STATE_READY), memory_order_relaxed);
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

    if ((task->peer == peer) &&
        (task->state < INSTANT_TASK_STATE_WAIT_COMPLETION))
    {
      // Tasks in state INSTANT_TASK_STATE_WAIT_COMPLETION should be removed by HandleCompletedRead() / HandleCompletedWrite()
      RemoveTask(replicator, task);
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
  request.wr_id   = number;
  request.next    = NULL;
  request.sg_list = &element;
  request.num_sge = 1;

  return ibv_post_srq_recv(card->queue1, &request, &temporary);
}

static int SubmitInitialReceivingBufferList(struct InstantCard* card)
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
      (card  = (struct InstantCard*)calloc(1, sizeof(struct InstantCard))))
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
          (SubmitInitialReceivingBufferList(card) == 0)))
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
        switch (completion->opcode)
        {
          case IBV_WC_SEND:
            ReleaseSharedBuffer(&replicator->buffers, (struct InstantSharedBuffer*)completion->wr_id);
            break;

          case IBV_WC_RECV:
            SubmitReceivingBuffer(card, completion->wr_id ^ 1ULL);
            HandleReceivedMessage(replicator, card->buffers[completion->wr_id], completion->byte_len, completion->imm_data, completion->status, completion->wc_flags & IBV_WC_WITH_IMM);
            break;

          case IBV_WC_RECV_RDMA_WITH_IMM:
            SubmitReceivingBuffer(card, completion->wr_id ^ 1ULL);
            HandleTranferredData(replicator, completion->imm_data, completion->status);
            break;

          case IBV_WC_RDMA_READ:
            HandleCompletedRead(replicator, (struct InstantTask*)completion->wr_id, completion->status);
            break;

          case IBV_WC_RDMA_WRITE:
            HandleCompletedWrite(replicator, (struct InstantTask*)completion->wr_id, completion->status);
            break;

          case IBV_WC_COMP_SWAP:
            HandleCompletedSwap(replicator, (struct InstantTask*)(completion->wr_id & INSTANT_TASK_ADDRESS_MASK), completion->wr_id & INSTANT_TASK_SLOT_MASK, completion->status);
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

        case IBV_WR_RDMA_READ:
          HandleCompletedRead(replicator, (struct InstantTask*)item->request.wr_id, IBV_WC_WR_FLUSH_ERR);
          break;

        case IBV_WR_RDMA_WRITE:
        case IBV_WR_RDMA_WRITE_WITH_IMM:
          HandleCompletedWrite(replicator, (struct InstantTask*)item->request.wr_id, IBV_WC_WR_FLUSH_ERR);
          break;

        case IBV_WR_ATOMIC_CMP_AND_SWP:
          HandleCompletedSwap(replicator, (struct InstantTask*)(item->request.wr_id & INSTANT_TASK_ADDRESS_MASK), item->request.wr_id & INSTANT_TASK_SLOT_MASK, IBV_WC_WR_FLUSH_ERR);
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

  printf("HandleEstablished id=%p\n", descriptor);

  peer = (struct InstantPeer*)descriptor->context;

  peer->state      = INSTANT_PEER_STATE_CONNECTED;
  peer->fails      = 0;
  peer->points[peer->round].rank = 0;

  CreateSyncingTask(replicator, peer);

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

    ClearTaskList(replicator, peer);
    ClearRequestQueue(replicator, &peer->queue);
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

  while ((atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE) &&
         (io_uring_submit_and_wait_timeout(&replicator->ring, &completion, 1, &interval, NULL) >= 0))
  {
    io_uring_for_each_cqe(&replicator->ring, head, completion)
    {
      switch (completion->user_data)
      {
        case 0ULL:
        case LIBURING_UDATA_TIMEOUT:
          break;

        case RING_TAG_TIMEOUT:
          TrackPeerList(replicator);
          TrackCookieList(replicator);
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

  atomic_fetch_or_explicit(&replicator->state, INSTANT_REPLICATOR_STATE_FAILURE, memory_order_relaxed);
  atomic_fetch_and_explicit(&replicator->state, ~(INSTANT_REPLICATOR_STATE_HOLD | INSTANT_REPLICATOR_STATE_READY), memory_order_relaxed);

  while ((syscall(SYS_futex, (uint32_t*)&replicator->state, FUTEX_WAKE_BITSET | FUTEX_PRIVATE_FLAG, INT_MAX, NULL, NULL, FUTEX_BITSET_MATCH_ANY) < 0) &&
         (errno == EINTR));

  return NULL;
}

struct InstantReplicator* CreateInstantReplicator(int port, uuid_t identifier, const char* name, const char* secret, uint32_t limit, struct ReliableMonitor* next)
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
    replicator->secret         = strdup(secret);
    replicator->name           = strdup(name);
    replicator->limit          = limit;

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

    while (cookie = replicator->cookies)
    {
      replicator->cookies = cookie->next;
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

int RegisterRemoteInstantReplicator(struct InstantReplicator* replicator, uuid_t identifier, struct sockaddr* address, socklen_t length)
{
  int index;
  uint32_t count;
  struct InstantPeer* peer;
  struct InstantPeer* other;
  struct InstantPoint* point;

  pthread_mutex_lock(&replicator->lock);

  peer  = replicator->peers;
  count = 0;

  while ((peer != NULL) && (uuid_compare(peer->identifier, identifier) != 0))
  {
    ++ count;
    peer = peer->next;
  }

  if (peer == NULL)
  {
    if (count >= replicator->limit)
    {
      pthread_mutex_unlock(&replicator->lock);
      return -ENOSPC;
    }

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
