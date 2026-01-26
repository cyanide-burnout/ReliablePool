#ifndef RELIABLEINDEXER_H
#define RELIABLEINDEXER_H

#include "ReliablePool.h"
#include "RedBlackTree.h"
#include "HashMap.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define RELIABLE_COLLECT_UNUSED  (1 << 0)
#define RELIABLE_COLLECT_IN_USE  (1 << 1)

struct ReliableIndexKey
{
  char name[RELIABLE_MEMORY_NAME_LENGTH];
  uuid_t identifier;
};

struct ReliableIndexRecord
{
  struct ReliableIndexKey key;
  uint32_t number;
};

struct ReliableIndexer
{
  struct ReliableMonitor super;
  struct RedBlackTree* tree;
  struct HashMap* map;
  pthread_rwlock_t lock;
};

struct ReliableIndexer* CreateReliableIndexer(struct ReliableMonitor* next);
struct ReliableIndexer* GetReliableIndexer(struct ReliablePool* pool);
void ReleaseReliableIndexer(struct ReliableIndexer* indexer);

struct ReliablePool* FindReliablePool(struct ReliableIndexer* indexer, const char* name);

uint32_t FindReliableBlockNumber(struct ReliableIndexer* indexer, const char* name, uuid_t identifier);
uint32_t* CollectReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time, uint32_t flags);

int RemoveUnusedReliableBlockList(struct ReliableIndexer* indexer, struct ReliablePool* pool, struct timespec* time);

#ifdef __cplusplus
}
#endif

#endif
