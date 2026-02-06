#ifndef RELIABLEPOOL_H
#define RELIABLEPOOL_H

#include <stddef.h>
#include <stdint.h>
#include <time.h>
#include <uuid.h>

#ifndef __cplusplus
#include <pthread.h>
#include <stdatomic.h>
#define ATOMIC(type)  type _Atomic
#else
#include <new>
#include <memory>
#include <atomic>
#include <typeinfo>
#include <stdexcept>
#include <type_traits>
#define ATOMIC(type)  std::atomic<type>
#endif

#ifdef __cplusplus
extern "C"
{
#endif

#define RELIABLE_MEMORY_MAGIC        4
#define RELIABLE_MEMORY_NAME_LENGTH  8

#define RELIABLE_TYPE_FREE             0
#define RELIABLE_TYPE_RECOVERABLE      1
#define RELIABLE_TYPE_NON_RECOVERABLE  2

#define RELIABLE_FLAG_RESET  (1UL << 0)

#define RELIABLE_MONITOR_POOL_CREATE     0
#define RELIABLE_MONITOR_POOL_RELEASE    1
#define RELIABLE_MONITOR_SHARE_CREATE    2
#define RELIABLE_MONITOR_SHARE_DESTROY   3
#define RELIABLE_MONITOR_BLOCK_ALLOCATE  4
#define RELIABLE_MONITOR_BLOCK_RECOVER   5
#define RELIABLE_MONITOR_BLOCK_ATTACH    6
#define RELIABLE_MONITOR_BLOCK_RELEASE   7
#define RELIABLE_MONITOR_BLOCK_RESERVE   8
#define RELIABLE_MONITOR_BLOCK_FREE      9

#define RELIABLE_MONITOR_CLOSURE_COUNT  4

#define RELIABLE_WEIGHT_WEAK    (1UL << 0)
#define RELIABLE_WEIGHT_STRONG  (1UL << 32)

struct ReliablePool;
struct ReliableShare;
struct ReliableBlock;

typedef int (*ReliableRecoveryFunction)(struct ReliablePool* pool, struct ReliableBlock* block, void* closure);
typedef void (*ReliableMonitorFunction)(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure);

struct ReliableBlock
{
  uint32_t type;              // ┌ RELIABLE_TYPE_*
  uint32_t number;            // │ Block number
  ATOMIC(uint64_t) next;      // │ Next free block
  ATOMIC(uint32_t) tag;       // │ Local version tag
  ATOMIC(uint32_t) count;     // └ Count of references
  ATOMIC(uint64_t) mark;      // ┌ Remote replication mark/fence
  uuid_t identifier;          // └ Global block identifier
  ATOMIC(uint32_t) control;   //   CRC32C of data if block under tracking
  uint32_t reserved[2];       //   Reserved for future use
  uint32_t length;            //   Data length, can be filled by user
  uint8_t data[0];
};

struct ReliableMemory  // Sturcture behind the mmap
{
  uint32_t magic;                          // RELIABLE_MEMORY_MAGIC
  uint32_t size;                           // Block size including sizeof(struct ReliableBlock)
  ATOMIC(uint64_t) free;                   // First free block
  ATOMIC(uint32_t) length;                 // Length of pool in blocks
  uint32_t reserved;                       //
  char name[RELIABLE_MEMORY_NAME_LENGTH];  //
  uint8_t data[0];
};

struct ReliableShare
{
  struct ReliableMemory* memory;                   // mmap-ed address
  size_t size;                                     // mmap-ed memory size
  ATOMIC(uint64_t) weight;                         // Weight of references
  void* closures[RELIABLE_MONITOR_CLOSURE_COUNT];  // Reserved for monitor/replicator
};

struct ReliableMonitor
{
  struct ReliableMonitor* next;
  ReliableMonitorFunction function;
  void* closure;
  const char* name;
};

struct ReliablePool
{
  int handle;                                      // Handle of file or memfd
  time_t time;                                     // Object creation time
  size_t grain;                                    // Growth of expansion
  struct ReliableShare* share;                     // Active memory share
  pthread_rwlock_t lock;                           // Lock for expand operation
  ATOMIC(uint32_t) count;                          // Count of references
  ATOMIC(struct ReliableMonitor*) monitor;         // First monitor
  void* closures[RELIABLE_MONITOR_CLOSURE_COUNT];  // Reserved for monitor/replicator
};

struct ReliableDescriptor
{
  struct ReliablePool* pool;
  struct ReliableShare* share;
  struct ReliableBlock* block;
};

struct ReliablePool* CreateReliablePool(int handle, const char* name, size_t length, uint32_t flags, struct ReliableMonitor* monitor, ReliableRecoveryFunction function, void* closure);
void ReleaseReliablePool(struct ReliablePool* pool);
int UpdateReliablePool(struct ReliablePool* pool);

void* AllocateReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, int type);
void* AttachReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, uint32_t number, uint32_t tag);
void* ShareReliableBlock(const struct ReliableDescriptor* source, struct ReliableDescriptor* destination);
void ReleaseReliableBlock(struct ReliableDescriptor* descriptor, int type);

// Should be used inside the call of ReliableRecoveryFunction
void* RecoverReliableBlock(struct ReliableDescriptor* descriptor, struct ReliablePool* pool, struct ReliableBlock* block);

// Following functions should be use internally

void CallReliableMonitor(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block);

uint32_t ReserveReliableBlock(struct ReliablePool* pool, uuid_t identifier, int type);
int FreeReliableBlock(struct ReliablePool* pool, uint32_t number, uuid_t identifier);

struct ReliableShare* MakeReliableShareCopy(struct ReliablePool* pool, struct ReliableShare* share);
void RetireReliableShare(struct ReliableShare* share);
void RetireReliablePool(struct ReliablePool* pool);

// C++ helpers

#ifdef __cplusplus
}

template<typename Type> class ReliableHolder
{
  public:

    ReliableHolder()                                                          noexcept  { descriptor = { };                                      }
    ReliableHolder(ReliableHolder&& other)                                    noexcept  { descriptor = other.descriptor, other.descriptor = { }; }
    ReliableHolder(const ReliableHolder& other)                               noexcept  { ShareReliableBlock(&other.descriptor, &descriptor);    }
    ReliableHolder(struct ReliableDescriptor& other)                          noexcept  { ShareReliableBlock(&other, &descriptor);               }
    ReliableHolder(struct ReliablePool* pool, int type)                       noexcept  { AllocateReliableBlock(&descriptor, pool, type);        }
    ReliableHolder(struct ReliablePool* pool, struct ReliableBlock* block)    noexcept  { RecoverReliableBlock(&descriptor, pool, block);        }
    ReliableHolder(struct ReliablePool* pool, uint32_t number, uint32_t tag)  noexcept  { AttachReliableBlock(&descriptor, pool, number, tag);   }
    ~ReliableHolder()                                                                   { ReleaseReliableBlock(&descriptor, RELIABLE_TYPE_FREE); }

    void release(int type)  { ReleaseReliableBlock(&descriptor, type);           }
    Type* get()             { return static_cast<Type*>(descriptor.block->data); }

    operator bool()         { return descriptor.pool && descriptor.block;        }
    Type* operator->()      { return static_cast<Type*>(descriptor.block->data); }

    ReliableHolder& operator=(const ReliableHolder& other)
    {
      if (this != &other)
      {
        ReleaseReliableBlock(&descriptor, RELIABLE_TYPE_FREE);
        ShareReliableBlock(&other.descriptor, &descriptor);
      }
      return *this;
    }

    ReliableHolder& operator=(ReliableHolder&& other)
    {
      if (this != &other)
      {
        ReleaseReliableBlock(&descriptor, RELIABLE_TYPE_FREE);
        descriptor       = other.descriptor;
        other.descriptor = { };
      }
      return *this;
    }

  private:

    struct ReliableDescriptor descriptor;

};

template<typename Type> class ReliableAllocator
{
  public:

    typedef Type value_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    template <typename Other> struct rebind
    {
      typedef std::allocator<Other> other;
    };

    struct Container
    {
      struct ReliableDescriptor descriptor;
      std::size_t type;
      Type data;
    };

    ReliableAllocator() noexcept
    {
      initial = RELIABLE_TYPE_RECOVERABLE;
      final   = RELIABLE_TYPE_FREE;
      pool    = nullptr;
      block   = nullptr;
    }

    ReliableAllocator(ReliableAllocator& other) noexcept
    {
      initial = other.initial;
      final   = other.final;
      pool    = other.pool;
      block   = nullptr;
    }

    ReliableAllocator(struct ReliablePool* pool, int initial = RELIABLE_TYPE_RECOVERABLE, int final = RELIABLE_TYPE_FREE) noexcept
    {
      this->initial = initial;
      this->final   = final;
      this->pool    = pool;
      this->block   = nullptr;
    }

    void assign(struct ReliablePool* pool, int initial = RELIABLE_TYPE_RECOVERABLE, int final = RELIABLE_TYPE_FREE) noexcept
    {
      this->initial = initial;
      this->final   = final;
      this->pool    = pool;
      this->block   = nullptr;
    }

    void assign(struct ReliablePool* pool, struct ReliableBlock* block) noexcept
    {
      this->pool  = pool;
      this->block = block;
    }

    [[nodiscard]] constexpr Type* allocate(std::size_t count)
    {
      Container* container;
      struct ReliableDescriptor descriptor;

      if ((count <= 1) &&
          (pool != nullptr) &&
          (pool->share->memory->size >= size))
      {
        if ((block != nullptr) &&
            (container = reinterpret_cast<Container*>(block->data)))
        {
          if ((block->length   != sizeof(Container)) ||
              (container->type != typeid(Type).hash_code()))
          {
            block = nullptr;
            throw std::bad_alloc();
          }

          RecoverReliableBlock(&container->descriptor, pool, block);
          container->type = typeid(Type).hash_code();
          block           = nullptr;
          return &container->data;
        }

        if (container = reinterpret_cast<Container*>(AllocateReliableBlock(&descriptor, pool, initial)))
        {
          descriptor.block->length = sizeof(Container);
          container->type          = typeid(Type).hash_code();
          container->descriptor    = descriptor;
          return &container->data;
        }
      }

      throw std::bad_alloc();
    }

    void deallocate(Type* pointer, std::size_t count) noexcept
    {
      Container* container;
      struct ReliableDescriptor descriptor;

      container  = reinterpret_cast<Container*>(reinterpret_cast<char*>(pointer) - offsetof(Container, data));
      descriptor = container->descriptor;

      ReleaseReliableBlock(&descriptor, final);
    }

    static constexpr std::size_t length = sizeof(Container);
    static constexpr std::size_t size   = sizeof(struct ReliableBlock) + sizeof(Container);

  private:

    int final;
    int initial;
    struct ReliablePool* pool;
    struct ReliableBlock* block;

};

template <typename Type1, typename Type2> bool operator==(const ReliableAllocator<Type1>& allocator1, const ReliableAllocator<Type2>& allocator2) noexcept
{
  return std::is_same<Type1, Type2>::value;
};

#endif

#endif
