#include "ReliableIndexer.h"

#include <errno.h>
#include <malloc.h>
#include <string.h>
#include <sys/mman.h>

_Static_assert(offsetof(struct ReliableIndexer, super) == 0, "ReliableIndexer must embed ReliableMonitor as first field");

static const char* MonitorName = "ReliableIndexer";

static void ReleaseIndexKey(void* key, void* data)
{
  free(key);
}

static int CompareIndexKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2)
{
  return memcmp(key1, key2, sizeof(struct ReliableIndexKey));
}

static void ReleaseReliableIndexRecord(struct RedBlackTree* tree, void* value)
{
  free(value);
}

static int AddPool(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct ReliableShare* share)
{
  struct ReliableMemory* memory;
  int result;
  char* key;

  result = 0;
  memory = share->memory;

  if (key = (char*)calloc(1, RELIABLE_MEMORY_NAME_LENGTH))
  {
    memcpy(key, memory->name, RELIABLE_MEMORY_NAME_LENGTH);

    pthread_rwlock_wrlock(&indexer->lock);
    result = (PutIntoHashMap(indexer->map, key, RELIABLE_MEMORY_NAME_LENGTH, pool) == HASHMAP_SUCCESS);
    pthread_rwlock_unlock(&indexer->lock);

    if (result == 0)
    {
      // Put failed
      free(key);
    }
  }

  return result;
}

static void RemovePool(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct ReliableShare* share)
{
  struct ReliableMemory* memory;

  memory = share->memory;

  pthread_rwlock_wrlock(&indexer->lock);
  RemoveFromHashMap(indexer->map, memory->name, RELIABLE_MEMORY_NAME_LENGTH, NULL);
  pthread_rwlock_unlock(&indexer->lock);
}

static int AddBlock(struct ReliableIndexer* indexer, struct ReliableShare* share, struct ReliableBlock* block)
{
  struct ReliableIndexRecord* record;
  struct ReliableMemory* memory;
  int result;

  result = 0;
  memory = share->memory;

  if (record = (struct ReliableIndexRecord*)calloc(1, sizeof(struct ReliableIndexRecord)))
  {
    record->number = block->number;
    memcpy(record->key.name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);
    uuid_copy(record->key.identifier, block->identifier);

    pthread_rwlock_wrlock(&indexer->lock);
    RemoveFromRedBlackTree(indexer->tree, &record->key, NULL);
    result = PutIntoRedBlackTree(indexer->tree, &record->key, record);
    pthread_rwlock_unlock(&indexer->lock);

    if (result == 0)
    {
      // Put failed
      free(record);
    }
  }

  return result;
}

static void RemoveBlock(struct ReliableIndexer* indexer, struct ReliableShare* share, struct ReliableBlock* block)
{
  struct ReliableIndexKey key;
  struct ReliableMemory* memory;

  memory = share->memory;

  memset(&key, 0, sizeof(struct ReliableIndexKey));
  memcpy(key.name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);
  uuid_copy(key.identifier, block->identifier);

  pthread_rwlock_wrlock(&indexer->lock);
  RemoveFromRedBlackTree(indexer->tree, &key, NULL);
  pthread_rwlock_unlock(&indexer->lock);
}

static void RemoveBlockList(struct ReliableIndexer* indexer, struct ReliableShare* share)
{
  struct ReliableIndexKey key;
  struct ReliableMemory* memory;
  struct RedBlackIterator iterator;
  struct ReliableIndexRecord* record;

  memory = share->memory;

  memset(&key, 0, sizeof(struct ReliableIndexKey));
  memcpy(key.name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);

  pthread_rwlock_wrlock(&indexer->lock);

  while ((record = (struct ReliableIndexRecord*)StartRedBlackIteratorFromValue(&iterator, indexer->tree, &key, REDBLACK_LINK_RIGHT)) &&
         (memcmp(record->key.name, key.name, RELIABLE_MEMORY_NAME_LENGTH) == 0))
  {
    //
    RemoveFromRedBlackTree(indexer->tree, &record->key, record);
  }

  pthread_rwlock_unlock(&indexer->lock);
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct ReliableIndexer* indexer;

  indexer = (struct ReliableIndexer*)closure;

  switch (event)
  {
    case RELIABLE_MONITOR_POOL_CREATE:
      AddPool(indexer, pool, share);
      break;

    case RELIABLE_MONITOR_POOL_RELEASE:
      RemoveBlockList(indexer, share);
      RemovePool(indexer, pool, share);
      break;

    case RELIABLE_MONITOR_BLOCK_ALLOCATE:
      uuid_generate(block->identifier);

    case RELIABLE_MONITOR_BLOCK_RECOVER:
    case RELIABLE_MONITOR_BLOCK_RESERVE:
      AddBlock(indexer, share, block);
      break;

    case RELIABLE_MONITOR_BLOCK_RELEASE:
    case RELIABLE_MONITOR_BLOCK_FREE:
      RemoveBlock(indexer, share, block);
      break;
  }
}

struct ReliableIndexer* CreateReliableIndexer(struct ReliableMonitor* next)
{
  struct ReliableIndexer* indexer;

  if (indexer = (struct ReliableIndexer*)calloc(1, sizeof(struct ReliableIndexer)))
  {
    indexer->super.next     = next;
    indexer->super.closure  = indexer;
    indexer->super.function = HandleMonitorEvent;
    indexer->super.name     = MonitorName;

    indexer->map  = CreateHashMap(ReleaseIndexKey);
    indexer->tree = CreateRedBlackTree(CompareIndexKeys, ReleaseReliableIndexRecord, indexer);

    pthread_rwlock_init(&indexer->lock, NULL);
  }

  return indexer;
}

struct ReliableIndexer* GetReliableIndexer(struct ReliablePool* pool)
{
  struct ReliableMonitor* monitor;

  monitor = atomic_load_explicit(&pool->monitor, memory_order_relaxed);

  while ((monitor       != NULL) &&
         (monitor->name != MonitorName))
  {
    //
    monitor = monitor->next;
  }

  return (struct ReliableIndexer*)monitor;
}

void ReleaseReliableIndexer(struct ReliableIndexer* indexer)
{
  if (indexer != NULL)
  {
    ReleaseHashMap(indexer->map);
    ReleaseRedBlackTree(indexer->tree);
    pthread_rwlock_destroy(&indexer->lock);
    free(indexer);
  }
}

struct ReliablePool* FindReliablePool(struct ReliableIndexer* indexer, const char* name)
{
  char key[RELIABLE_MEMORY_NAME_LENGTH];
  struct ReliablePool* pool;

  pool = NULL;

  memset(key, 0, RELIABLE_MEMORY_NAME_LENGTH);
  strncpy(key, name, RELIABLE_MEMORY_NAME_LENGTH);

  pthread_rwlock_rdlock(&indexer->lock);
  GetFromHashMap(indexer->map, key, RELIABLE_MEMORY_NAME_LENGTH, (void**)&pool);
  pthread_rwlock_unlock(&indexer->lock);

  return pool;
}

uint32_t FindReliableBlockNumber(struct ReliableIndexer* indexer, const char* name, uuid_t identifier)
{
  struct ReliableIndexRecord* record;
  struct ReliableIndexKey key;
  uint32_t number;

  memset(&key, 0, sizeof(struct ReliableIndexKey));
  strncpy(key.name, name, RELIABLE_MEMORY_NAME_LENGTH);
  uuid_copy(key.identifier, identifier);

  pthread_rwlock_rdlock(&indexer->lock);
  record = (struct ReliableIndexRecord*)GetFromRedBlackTree(indexer->tree, &key);
  number = (record != NULL) ? record->number : UINT32_MAX;
  pthread_rwlock_unlock(&indexer->lock);

  return number;
}

uint32_t* CollectReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time, uint32_t flags)
{
  struct ReliableIndexRecord* record;
  struct RedBlackIterator iterator;
  struct ReliableMemory* memory;
  struct ReliableShare* share;
  struct ReliableBlock* block;
  struct ReliableIndexKey key;
  struct RedBlackTree* tree;
  uint32_t* entry;
  uint32_t* list;
  uint32_t length;
  uint32_t count;
  uint64_t cut;

  list = NULL;
  cut  = (time != NULL) ? (((uint64_t)time->tv_sec * 1000000000ULL + (uint64_t)time->tv_nsec) & ~0xffffffULL) : UINT64_MAX;

  pthread_rwlock_rdlock(&pool->lock);
  share  = pool->share;
  memory = share->memory;
  atomic_fetch_add_explicit(&share->weight, RELIABLE_WEIGHT_WEAK, memory_order_relaxed);
  pthread_rwlock_unlock(&pool->lock);

  memset(&key, 0, sizeof(struct ReliableIndexKey));
  memcpy(key.name, memory->name, RELIABLE_MEMORY_NAME_LENGTH);

  pthread_rwlock_rdlock(&indexer->lock);

  if ((tree = indexer->tree) &&
      (list = (uint32_t*)malloc((tree->size + 1) * sizeof(uint32_t))))
  {
    entry  = list;
    length = atomic_load_explicit(&memory->length, memory_order_acquire);
    record = (struct ReliableIndexRecord*)StartRedBlackIteratorFromValue(&iterator, indexer->tree, &key, REDBLACK_LINK_RIGHT);

    while ((record != NULL) &&
           (memcmp(record->key.name, key.name, RELIABLE_MEMORY_NAME_LENGTH) == 0))
    {
      if (record->number < length)
      {
        block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)record->number);
        count = atomic_load_explicit(&block->count, memory_order_acquire);

        if ((block->type != RELIABLE_TYPE_FREE) &&
            ((flags & RELIABLE_COLLECT_UNUSED) && (count == 0)  ||
             (flags & RELIABLE_COLLECT_IN_USE) && (count != 0)) &&
            (cut > atomic_load_explicit(&block->mark, memory_order_relaxed)))
        {
          //
          *(entry ++) = record->number;
        }
      }

      record = (struct ReliableIndexRecord*)MoveNextRedBlackValue(&iterator);
    }

    *entry = UINT32_MAX;
  }

  pthread_rwlock_unlock(&indexer->lock);

  ReleaseReliableShare(share);

  return list;
}

int RemoveUnusedReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time)
{
  uuid_t identifier;
  uint32_t* entry;
  uint32_t* list;
  int count;

  list = CollectReliableBlockList(indexer, pool, time, RELIABLE_COLLECT_UNUSED);

  if (list != NULL)
  {
    count = 0;
    entry = list;
    uuid_clear(identifier);

    while (*entry != UINT32_MAX)
    {
      count += (FreeReliableBlock(pool, *entry, identifier) == 0);
      entry ++;
    }

    free(list);
    return count;
  }

  return -ENOMEM;
}
