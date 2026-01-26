#ifndef HASHHASHMAP_H
#define HASHHASHMAP_H

#include <stddef.h>
#include <stdint.h>

/*

  Originally by Elliot C Back - http://elliottback.com/wp/hashmap-implementation-in-c/
  Modified by Pete Warden to fix a serious performance problem, support strings as keys
  and removed thread synchronization - https://github.com/petewarden/c_hashmap

  Re-written by Artem Prilutskiy (cyanide.burnout@gmail.com)

*/

#ifdef __cplusplus
#include <functional>

extern "C"
{
#endif

#define HASHMAP_SUCCESS   0
#define HASHMAP_MEMORY   -1  // Out of Memory
#define HASHMAP_FULL     -2  // Map is full
#define HASHMAP_MISSING  -3  // No such element

#define HASHMAP_FREE     0
#define HASHMAP_ENGAGED  1

typedef void (*ReleaseHashMapItem)(void* key, void* data);
typedef int (*HandleHashMapItem)(void* key, size_t size, void* data, void* argument1, void* argument2);

struct HashMapData
{
  uint16_t state;           // Item state: HASHMAP_FREE or HASHMAP_ENGAGED
  uint16_t size;            // Key size (who use key bigger than 64K?)
  uint32_t hash;            // Pre-calculated hash value
} __attribute__((packed));  // Total size: 64 bits

union HashMapVector
{
  struct HashMapData data;  // Plain data
  uint64_t value;           // Vector value
};

struct HashMapItem
{
  union HashMapVector vector;  // Item identification vector
  void* data;                  // Reference to data (data will not be copied)
  void* key;                   // Reference to key (key will not be copied)
};

struct HashMap
{
  size_t size;    // Total count of elements
  size_t mask;    // Mask to speed up search
  size_t length;  // Count of elements in use
  struct HashMapItem* data;
  ReleaseHashMapItem release;
};

struct HashMap* CreateHashMap(ReleaseHashMapItem function);
void ReleaseHashMap(struct HashMap* map);

int GetFromHashMap(struct HashMap* map, const void* key, size_t size, void** value);
int PutIntoHashMap(struct HashMap* map, const void* key, size_t size, const void* value);
int RemoveFromHashMap(struct HashMap* map, const void* key, size_t size, const void* value);
void IterateThroughHashMap(struct HashMap* map, HandleHashMapItem function, void* argument1, void* argument2);

#ifdef __cplusplus
}
#endif

#endif
