#include "HashMap.h"

#include <malloc.h>
#include <string.h>

#include "CRC32C.h"

#ifdef __OPTIMIZE__
#define INLINE  inline
#else
#define INLINE  static
#endif

#define INITIAL_SIZE          256
#define MAXIMUM_CHAIN_LENGTH  8

struct HashMap* CreateHashMap(ReleaseHashMapItem function)
{
  struct HashMap* map;

  map = (struct HashMap*)calloc(1, sizeof(struct HashMap));

  if (map != NULL)
  {
    map->data    = (struct HashMapItem*)calloc(INITIAL_SIZE, sizeof(struct HashMapItem));
    map->size    = INITIAL_SIZE;
    map->mask    = map->size - 1;
    map->length  = 0;
    map->release = function;

    if (map->data == NULL)
    {
      free(map);
      map = NULL;
    }
  }

  return map;
}

static void ReleaseAll(struct HashMap* map)
{
  struct HashMapItem* item;

  if (map->release != NULL)
  {
    for (item = map->data; map->length > 0; item ++)
    {
      if (item->vector.value != HASHMAP_FREE)
      {
        map->release(item->key, item->data);
        map->length --;
      }
    }
  }
}

void ReleaseHashMap(struct HashMap* map)
{
  if (map != NULL)
  {
    ReleaseAll(map);
    free(map->data);
    free(map);
  }
}

INLINE uint32_t GetKeyHash(const void* key, size_t size)
{
  switch (size)
  {
    case 0:                 return 0;
    case sizeof(uint8_t):   return GetHash(*(uint8_t*)key);
    case sizeof(uint16_t):  return GetHash(*(uint16_t*)key);
    case sizeof(uint32_t):  return GetHash(*(uint32_t*)key);
    default:                return GetCRC32C((uint8_t*)key, size, 0);
  }
}

INLINE struct HashMapItem* GetWritePosition(struct HashMap* map, register union HashMapVector vector, const void* key)
{
  size_t index;
  size_t position;
  struct HashMapItem* item;
  struct HashMapItem* result;

  // If full, return immediately

  result = NULL;

  if (map->length < map->size / 2)
  {
    position = vector.data.hash & map->mask;

    for (index = 0; index < MAXIMUM_CHAIN_LENGTH; index ++)
    {
      item = map->data + position;

      if ((item->vector.value == vector.value) &&
          (memcmp(item->key, key, vector.data.size) == 0))
      {
        result = item;
        break;
      }

      if ((item->vector.value == HASHMAP_FREE) &&
          (result == NULL))
      {
        result = item;
        // Continue to search for duplicates
      }

      position ++;
      position &= map->mask;
    }
  }

  return result;
}

static int ExpandHashMap(struct HashMap* map)
{
  size_t mask;
  size_t length;
  size_t position;
  struct HashMapItem* item;
  struct HashMapItem* limit;

  item = (struct HashMapItem*)realloc(map->data, map->size * 2 * sizeof(struct HashMapItem));

  if (item == NULL)
    return HASHMAP_MEMORY;

  memcpy(item + map->size, item, map->size * sizeof(struct HashMapItem));

  mask   = map->size;
  limit  = item + MAXIMUM_CHAIN_LENGTH - 1;
  length = map->length;

  map->size *= 2;
  map->mask  = map->size - 1;
  map->data  = item;

  // Clean overlapped area (00* and 11* should stay in lower part, 01* and 10* should stay in upper part)

  for ( ; (item < limit) && (length > 0); item ++)
  {
    if (item->vector.value != HASHMAP_FREE)
    {
      position = (item->vector.data.hash ^ (item->vector.data.hash << 1)) & mask;
      item[position ^ mask].vector.value = HASHMAP_FREE;
      length --;
    }
  }

  // Clean the rest (0* should stay in lower part, 1* should stay in upper part)

  for ( ; length > 0; item ++)
  {
    if (item->vector.value != HASHMAP_FREE)
    {
      position = item->vector.data.hash & mask;
      item[position ^ mask].vector.value = HASHMAP_FREE;
      length --;
    }
  }

  return HASHMAP_SUCCESS;
}

int GetFromHashMap(struct HashMap* map, const void* key, size_t size, void** value)
{
  size_t index;
  size_t position;
  struct HashMapItem* item;
  union HashMapVector vector;

  vector.data.state = HASHMAP_ENGAGED;
  vector.data.size  = size;
  vector.data.hash  = GetKeyHash(key, size);
  position          = vector.data.hash & map->mask;

  for (index = 0; index < MAXIMUM_CHAIN_LENGTH; index ++)
  {
    item = map->data + position;

    if ((item->vector.value == vector.value) &&
        (memcmp(item->key, key, size) == 0))
    {
      *value = item->data;
      return HASHMAP_SUCCESS;
    }

    position ++;
    position &= map->mask;
  }

  return HASHMAP_MISSING;
}

int PutIntoHashMap(struct HashMap* map, const void* key, size_t size, const void* value)
{
  struct HashMapItem* item;
  union HashMapVector vector;

  vector.data.state = HASHMAP_ENGAGED;
  vector.data.size  = size;
  vector.data.hash  = GetKeyHash(key, size);

  for ( ; ; )
  {
    if (item = GetWritePosition(map, vector, key))  break;
    if (ExpandHashMap(map) == HASHMAP_MEMORY)       return HASHMAP_MEMORY;
  }

  if ((map->release != NULL) &&
      (item->vector.value != HASHMAP_FREE))
    map->release(item->key, item->data);

  map->length -= item->vector.data.state;

  item->vector.value = vector.value;
  item->data         = (void*)value;
  item->key          = (void*)key;

  map->length ++;

  return HASHMAP_SUCCESS;
}

int RemoveFromHashMap(struct HashMap* map, const void* key, size_t size, const void* value)
{
  size_t index;
  size_t position;
  struct HashMapItem* item;
  union HashMapVector vector;

  vector.data.state = HASHMAP_ENGAGED;
  vector.data.size  = size;
  vector.data.hash  = GetKeyHash(key, size);
  position          = vector.data.hash & map->mask;

  for (index = 0; index < MAXIMUM_CHAIN_LENGTH; index ++)
  {
    item = map->data + position;

    if ((item->vector.value == vector.value) &&
        (memcmp(item->key, key, size) == 0))
    {
      if ((value != NULL) &&
          (value != item->data))
        return HASHMAP_MISSING;

      if (map->release != NULL)
        map->release(item->key, item->data);

      item->vector.value = HASHMAP_FREE;
      map->length --;

      return HASHMAP_SUCCESS;
    }

    position ++;
    position &= map->mask;
  }

  return HASHMAP_MISSING;
}

void IterateThroughHashMap(struct HashMap* map, HandleHashMapItem function, void* argument1, void* argument2)
{
  size_t length;
  struct HashMapItem* item;

  for (length = map->length, item = map->data; length > 0; item ++)
  {
    if (item->vector.value != HASHMAP_FREE)
    {
      length --;
      if (function(item->key, item->vector.data.size, item->data, argument1, argument2))
      {
        if (map->release != NULL)
          map->release(item->key, item->data);

        item->vector.value = HASHMAP_FREE;
        map->length --;
      }
    }
  }
}
