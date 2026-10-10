#define _GNU_SOURCE

#include <time.h>
#include <netdb.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sys/random.h>

#include "FastRing.h"
#include "FastAvahiPoll.h"

#include "CRC32C.h"

#include "ReliablePool.h"
#include "ReliableTracker.h"
#include "ReliableIndexer.h"
#include "ReliableWaiter.h"

#include "InstantReplicator.h"
#include "InstantDiscovery.h"
#include "InstantWaiter.h"

#define POOL_NAME       "Test"
#define SERVICE_NAME    "Replication"
#define PAYLOAD_MAGIC   0x52504c54
#define MESSAGE_MAGIC   0x52504d53
#define STREAM_COUNT    16
#define HISTORY_LENGTH  (1 << 20)
#define SAMPLE_LIMIT    (1 << 24)
#define WRITE_BUDGET    500000  // Nanoseconds of writing per timer tick, keeps the loop responsive for flushes

#define STATE_RUNNING   0
#define STATE_QUIESCE   1
#define STATE_STOPPED   2

struct Payload
{
  uint32_t control;      // CRC32C of the rest of payload
  uint32_t magic;        // PAYLOAD_MAGIC
  uint32_t length;       // Payload length including header
  uint32_t seed;         // Seed of the fill
  uint64_t sequence;     // Author-wide monotonic write counter
  struct timespec time;  // CLOCK_REALTIME of the write
  uuid_t author;         // Identifier of the writing node
  uint8_t fill[0];
};

struct Message
{
  uint32_t magic;        // MESSAGE_MAGIC
  uint32_t reserved;     //
  uint64_t incarnation;  // Random per process, a restarted author starts its sequence again
  uint64_t sequence;     // Author-wide counter of transmitted user messages
  uuid_t author;         // Identifier of the sending node
};

struct Stream
{
  uuid_t author;
  uint64_t incarnation;
  uint64_t sequence;     // Last received sequence
  int broken;            // The connection to the author was broken since the last received message
};

struct History
{
  uuid_t identifier;
  uint64_t sequence;
};

struct Released
{
  uint32_t count;
  uint32_t length;
  uuid_t* data;
};

struct Counters
{
  ATOMIC(uint64_t) writes;
  ATOMIC(uint64_t) frees;
  ATOMIC(uint64_t) arrivals;
  ATOMIC(uint64_t) removals;
  ATOMIC(uint64_t) damages;
  ATOMIC(uint64_t) corrupts;
  ATOMIC(uint64_t) stales;
  ATOMIC(uint64_t) repeats;
  ATOMIC(uint64_t) connects;
  ATOMIC(uint64_t) disconnects;
  ATOMIC(uint64_t) messages;   // User messages transmitted
  ATOMIC(uint64_t) refused;    // User messages refused by TransmitInstantReplicatorUserMessage()
  ATOMIC(uint64_t) received;   // User messages received
  ATOMIC(uint64_t) skipped;    // User messages missing before a connection to their author or across its disconnect
  ATOMIC(uint64_t) lost;       // User messages missing within a connection
  ATOMIC(uint64_t) disorders;  // User messages received out of order or malformed
  ATOMIC(uint64_t) waited;     // Longest TransmitInstantReplicatorUserMessage() in nanoseconds
};

struct Samples
{
  pthread_mutex_t lock;
  uint32_t count;
  uint32_t limit;
  int64_t* data;  // One-way latencies in nanoseconds, include clock offset between nodes
};

struct Context
{
  uuid_t identifier;
  uint32_t rate;
  uint32_t count;
  uint32_t size;
  uint32_t ratio;
  uint32_t duration;
  uint32_t quiescence;
  uint32_t messages;
  uint64_t sequence;
  uint64_t done;
  uint64_t transmitted;
  uint64_t posted;
  uint64_t incarnation;
  int peers;
  struct timespec launch;
  struct timespec start;
  struct timespec stop;
  struct ReliablePool* pool;
  struct InstantReplicator* replicator;
  struct ReliableDescriptor* descriptors;
  struct History* history;
  struct Released released;
  struct Counters total;
  struct Counters period;
  struct Samples overall;
  struct Samples recent;
  struct Stream streams[STREAM_COUNT];
};

static ATOMIC(int) signaled = { 0 };
static ATOMIC(int) requested = { 0 };

static void HandleSignal(int signal)
{
  if (signal == SIGUSR1)
  {
    atomic_store_explicit(&requested, 1, memory_order_relaxed);
    return;
  }

  atomic_store_explicit(&signaled, 1, memory_order_relaxed);
}

static int64_t GetElapsedTime(const struct timespec* start, const struct timespec* stop)
{
  return (int64_t)(stop->tv_sec - start->tv_sec) * 1000000000LL + (stop->tv_nsec - start->tv_nsec);
}

static void FillPayload(uint8_t* data, size_t length, uint32_t seed)
{
  uint32_t value;

  value = seed | 1;

  while (length --)
  {
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;
    *(data ++) = (uint8_t)value;
  }
}

static int CheckFill(const uint8_t* data, size_t length, uint32_t seed)
{
  uint32_t value;

  value = seed | 1;

  while (length --)
  {
    value ^= value << 13;
    value ^= value >> 17;
    value ^= value << 5;

    if (*(data ++) != (uint8_t)value)
      return -1;
  }

  return 0;
}

#define PAYLOAD_VALID      0
#define PAYLOAD_SHORT      1
#define PAYLOAD_MAGIC_BAD  2
#define PAYLOAD_LENGTH     3
#define PAYLOAD_CONTROL    4
#define PAYLOAD_FILL       5

static const char* PayloadReasons[] = { "valid", "short", "magic", "length", "control", "fill" };

static int CheckPayload(const struct Payload* payload, uint32_t length)
{
  if (length < sizeof(struct Payload))                                                                                return PAYLOAD_SHORT;
  if (payload->magic != PAYLOAD_MAGIC)                                                                                return PAYLOAD_MAGIC_BAD;
  if (payload->length != length)                                                                                      return PAYLOAD_LENGTH;
  if (payload->control != GetCRC32C((const uint8_t*)payload + sizeof(uint32_t), length - sizeof(uint32_t), 0))  return PAYLOAD_CONTROL;
  if (CheckFill(payload->fill, length - sizeof(struct Payload), payload->seed) != 0)                                  return PAYLOAD_FILL;

  return PAYLOAD_VALID;
}

static void ReportCorruption(struct Context* context, struct ReliableBlock* block, int reason)
{
  static int count = 0;

  struct Payload* payload;
  char buffer[40];
  int consistent;

  if (count ++ >= 20)
    return;

  // Whether the payload is self-consistent under its own length, i.e. only the block length disagrees
  payload    = (struct Payload*)block->data;
  consistent = (payload->length >= sizeof(struct Payload)) && (payload->length <= context->size) && (CheckPayload(payload, payload->length) == PAYLOAD_VALID);

  uuid_unparse_lower(block->identifier, buffer);
  printf("CORRUPT block %u (%s) reason=%s length=%u payload_length=%u sequence=%llu block_control=%s payload_consistent=%d\n",
    block->number, buffer, (reason < 0) ? "oversize" : PayloadReasons[reason], block->length, payload->length, (unsigned long long)payload->sequence,
    (block->length <= context->size) && (GetCRC32C(block->data, block->length, 0) == atomic_load_explicit(&block->control, memory_order_relaxed)) ? "ok" : "bad",
    consistent);
}

static void AddCounter(ATOMIC(uint64_t)* total, ATOMIC(uint64_t)* period)
{
  atomic_fetch_add_explicit(total,  1, memory_order_relaxed);
  atomic_fetch_add_explicit(period, 1, memory_order_relaxed);
}

static void RaiseCounter(ATOMIC(uint64_t)* counter, uint64_t value)
{
  uint64_t current;

  current = atomic_load_explicit(counter, memory_order_relaxed);
  while ((current < value) &&
         !atomic_compare_exchange_weak_explicit(counter, &current, value, memory_order_relaxed, memory_order_relaxed));
}

static void AddSample(struct Samples* samples, int64_t value)
{
  pthread_mutex_lock(&samples->lock);

  if (samples->count < samples->limit)
    samples->data[samples->count ++] = value;

  pthread_mutex_unlock(&samples->lock);
}

static int CompareSamples(const void* value1, const void* value2)
{
  return (*(const int64_t*)value1 > *(const int64_t*)value2) - (*(const int64_t*)value1 < *(const int64_t*)value2);
}

static void SummarizeSamples(struct Samples* samples, int64_t* values, int reset)
{
  uint32_t count;

  memset(values, 0, 4 * sizeof(int64_t));
  pthread_mutex_lock(&samples->lock);

  if (count = samples->count)
  {
    qsort(samples->data, count, sizeof(int64_t), CompareSamples);

    values[0] = samples->data[0]                   / 1000;
    values[1] = samples->data[count / 2]           / 1000;
    values[2] = samples->data[count * 99ULL / 100] / 1000;
    values[3] = samples->data[count - 1]           / 1000;
  }

  if (reset)
    samples->count = 0;

  pthread_mutex_unlock(&samples->lock);
}

static void InitializeSamples(struct Samples* samples, uint32_t limit)
{
  pthread_mutex_init(&samples->lock, NULL);
  samples->count = 0;
  samples->limit = limit;
  samples->data  = (int64_t*)malloc(limit * sizeof(int64_t));
}

static void HandleArrival(struct Context* context, struct ReliableBlock* block)
{
  struct Payload* payload;
  struct History* history;
  struct timespec time;
  int reason;

  payload = (struct Payload*)block->data;

  AddCounter(&context->total.arrivals, &context->period.arrivals);

  if ((reason = (block->length > context->size) ? -1 : CheckPayload(payload, block->length)) != PAYLOAD_VALID)
  {
    AddCounter(&context->total.corrupts, &context->period.corrupts);
    ReportCorruption(context, block, reason);
    return;
  }

  clock_gettime(CLOCK_REALTIME, &time);
  AddSample(&context->overall, GetElapsedTime(&payload->time, &time));
  AddSample(&context->recent,  GetElapsedTime(&payload->time, &time));

  if (block->number < HISTORY_LENGTH)
  {
    // Arrivals are delivered from the replicator thread only
    history = context->history + block->number;

    if (uuid_compare(history->identifier, block->identifier) != 0)
    {
      uuid_copy(history->identifier, block->identifier);
      history->sequence = payload->sequence;
      return;
    }

    if (payload->sequence == history->sequence)
    {
      AddCounter(&context->total.repeats, &context->period.repeats);
      return;
    }

    if (payload->sequence < history->sequence)
    {
      AddCounter(&context->total.stales, &context->period.stales);
      return;
    }

    history->sequence = payload->sequence;
  }
}

static void HandleMonitorEvent(int event, struct ReliablePool* pool, struct ReliableShare* share, struct ReliableBlock* block, void* closure)
{
  struct Context* context;
  char buffer[40];

  context = (struct Context*)closure;

  switch (event)
  {
    case RELIABLE_MONITOR_BLOCK_ARRIVAL:
      HandleArrival(context, block);
      break;

    case RELIABLE_MONITOR_BLOCK_REMOVAL:
      AddCounter(&context->total.removals, &context->period.removals);
      break;

    case RELIABLE_MONITOR_BLOCK_DAMAGE:
      AddCounter(&context->total.damages, &context->period.damages);
      uuid_unparse_lower(block->identifier, buffer);
      printf("DAMAGE block %u (%s)\n", block->number, buffer);
      break;
  }
}

static void ReceiveMessage(struct Context* context, const struct Message* message, int length)
{
  struct Stream* stream;
  uint32_t index;

  if ((length         != sizeof(struct Message)) ||
      (message->magic != MESSAGE_MAGIC))
  {
    AddCounter(&context->total.disorders, &context->period.disorders);
    return;
  }

  for (index = 0; (index < STREAM_COUNT) && !uuid_is_null(context->streams[index].author) && (uuid_compare(context->streams[index].author, message->author) != 0); index ++);

  if (index == STREAM_COUNT)
  {
    // More authors than the test expects
    return;
  }

  stream = context->streams + index;

  if (uuid_is_null(stream->author) ||
      (stream->incarnation != message->incarnation))
  {
    // A new or restarted author, the stream may start after a gap: messages sent before the connection are not delivered
    uuid_copy(stream->author, message->author);
    stream->incarnation = message->incarnation;
    stream->sequence    = 0;
    stream->broken      = 1;
  }

  if (message->sequence <= stream->sequence)
  {
    // Messages are delivered in order within a connection, and a lost one is never sent again
    AddCounter(&context->total.disorders, &context->period.disorders);
    return;
  }

  if (stream->broken)
  {
    // Messages queued while the connection was down are lost with it
    atomic_fetch_add_explicit(&context->total.skipped,  message->sequence - stream->sequence - 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&context->period.skipped, message->sequence - stream->sequence - 1, memory_order_relaxed);
  }
  else
  {
    // Within a connection every message must arrive
    atomic_fetch_add_explicit(&context->total.lost,  message->sequence - stream->sequence - 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&context->period.lost, message->sequence - stream->sequence - 1, memory_order_relaxed);
  }

  AddCounter(&context->total.received, &context->period.received);

  stream->sequence = message->sequence;
  stream->broken   = 0;
}

static void BreakStream(struct Context* context, uuid_t author)
{
  uint32_t index;

  for (index = 0; index < STREAM_COUNT; index ++)
  {
    if (uuid_compare(context->streams[index].author, author) == 0)
    {
      // Events and messages come from the replicator thread in order, the next message may follow a gap
      context->streams[index].broken = 1;
    }
  }
}

static void HandleReplicatorEvent(int event, struct InstantPeer* peer, const char* data, int parameter, void* closure)
{
  struct Context* context;
  char buffer[40];

  context = (struct Context*)closure;

  switch (event)
  {
    case INSTANT_REPLICATOR_EVENT_CONNECTED:
      AddCounter(&context->total.connects, &context->period.connects);
      uuid_unparse_lower(peer->identifier, buffer);
      printf("CONNECTED %s vector %lld\n", buffer, (long long)peer->vector);
      break;

    case INSTANT_REPLICATOR_EVENT_DISCONNECTED:
      AddCounter(&context->total.disconnects, &context->period.disconnects);
      uuid_unparse_lower(peer->identifier, buffer);
      printf("DISCONNECTED %s\n", buffer);
      BreakStream(context, peer->identifier);
      break;

    case INSTANT_REPLICATOR_EVENT_USER_MESSAGE:
      ReceiveMessage(context, (const struct Message*)data, parameter);
      break;
  }
}

static void AppendReleased(struct Released* released, uuid_t identifier)
{
  uuid_t* data;

  if ((released->count == released->length) &&
      (data = (uuid_t*)realloc(released->data, (released->length + 4096) * sizeof(uuid_t))))
  {
    released->data    = data;
    released->length += 4096;
  }

  if (released->count < released->length)
  {
    // The dump lists released objects, so Compare.py can tell a confirmed removal from a lost object
    uuid_copy(released->data[released->count ++], identifier);
  }
}

static void GenerateOperation(struct Context* context)
{
  struct ReliableDescriptor* descriptor;
  struct Payload* payload;
  uint32_t numbers[3];
  uint32_t length;

  getrandom(numbers, sizeof(numbers), 0);

  descriptor = context->descriptors + (numbers[0] % context->count);

  if ((descriptor->block != NULL) &&
      ((numbers[1] % 100) < context->ratio))
  {
    AppendReleased(&context->released, descriptor->block->identifier);
    ReleaseReliableBlock(descriptor, RELIABLE_TYPE_FREE);
    AddCounter(&context->total.frees, &context->period.frees);
    return;
  }

  if ((descriptor->block == NULL) &&
      (AllocateReliableBlock(descriptor, context->pool, RELIABLE_TYPE_RECOVERABLE) == NULL))
  {
    printf("Allocation failed\n");
    return;
  }

  length  = sizeof(struct Payload) + numbers[2] % (context->size - sizeof(struct Payload) + 1);
  payload = (struct Payload*)descriptor->block->data;

  payload->magic    = PAYLOAD_MAGIC;
  payload->length   = length;
  payload->seed     = numbers[2];
  payload->sequence = ++ context->sequence;
  uuid_copy(payload->author, context->identifier);
  clock_gettime(CLOCK_REALTIME, &payload->time);
  FillPayload(payload->fill, length - sizeof(struct Payload), payload->seed);
  payload->control  = GetCRC32C((const uint8_t*)payload + sizeof(uint32_t), length - sizeof(uint32_t), 0);

  descriptor->block->length = length;

  AddCounter(&context->total.writes, &context->period.writes);
}

static void TransmitMessages(struct Context* context, struct timespec* time, uint64_t target)
{
  struct Message message;
  struct timespec before;
  struct timespec after;
  int result;

  memset(&message, 0, sizeof(struct Message));

  message.magic       = MESSAGE_MAGIC;
  message.incarnation = context->incarnation;
  uuid_copy(message.author, context->identifier);

  while (context->transmitted < target)
  {
    // Sent from the application thread with wait, the way an application relies on the delivery
    message.sequence = context->posted + 1;

    clock_gettime(CLOCK_MONOTONIC, &before);
    result = TransmitInstantReplicatorUserMessage(context->replicator, (const char*)&message, sizeof(struct Message), 1);
    clock_gettime(CLOCK_MONOTONIC, &after);

    RaiseCounter(&context->total.waited,  GetElapsedTime(&before, &after));
    RaiseCounter(&context->period.waited, GetElapsedTime(&before, &after));

    if (result == 0)
    {
      context->posted ++;
      AddCounter(&context->total.messages, &context->period.messages);
    }
    else
      AddCounter(&context->total.refused, &context->period.refused);

    context->transmitted ++;

    if (GetElapsedTime(time, &after) >= WRITE_BUDGET)
    {
      // Do not accumulate debt after a wait
      context->transmitted = target;
      break;
    }
  }
}

static void HandleWriteTimeout(struct FastRingDescriptor* descriptor)
{
  struct Context* context;
  struct timespec current;
  struct timespec time;
  uint64_t target;
  uint32_t count;

  context = (struct Context*)descriptor->closure;

  if (context->stop.tv_sec != 0)
    return;

  clock_gettime(CLOCK_MONOTONIC, &time);

  if (atomic_load_explicit(&context->total.connects, memory_order_relaxed) == 0)
  {
    // Writes made before the first connection arrive with the initial syncing, keep them out of the measurement
    context->start = time;
    return;
  }

  target = (uint64_t)GetElapsedTime(&context->start, &time) * context->rate / 1000000000ULL;
  count  = 0;

  while (context->done < target)
  {
    GenerateOperation(context);
    context->done ++;

    if ((++ count & 63) == 0)
    {
      clock_gettime(CLOCK_MONOTONIC, &current);

      if (GetElapsedTime(&time, &current) >= WRITE_BUDGET)
      {
        // Do not accumulate debt when the node cannot keep up
        context->done = target;
        break;
      }
    }
  }

  TransmitMessages(context, &time, (uint64_t)GetElapsedTime(&context->start, &time) * context->messages / 1000000000ULL);
}

static void ComputeDigest(struct Context* context, uint32_t* count, uint32_t* own, uint32_t* stamped, uint32_t* damaged, uint64_t* digest)
{
  struct ReliableMemory* memory;
  struct ReliableBlock* block;
  struct Payload* payload;
  uint32_t number;
  uint32_t value1;
  uint32_t value2;

  *count   = 0;
  *own     = 0;
  *stamped = 0;
  *damaged = 0;
  *digest  = 0;

  pthread_rwlock_rdlock(&context->pool->lock);

  memory = context->pool->share->memory;

  for (number = 0; number < atomic_load_explicit(&memory->length, memory_order_relaxed); ++ number)
  {
    block = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);

    if (atomic_load_explicit(&block->type, memory_order_relaxed) == RELIABLE_TYPE_FREE)
    {
      // Free blocks are expected to have cleared replication metadata
      *stamped += (atomic_load_explicit(&block->mark, memory_order_relaxed) != 0) || (atomic_load_explicit(&block->hint, memory_order_relaxed) != 0);
      continue;
    }

    // Outside the barrier a pending hint means a copy damaged by a transfer and not repaired yet
    *damaged += (atomic_load_explicit(&block->hint, memory_order_relaxed) & 1ULL) != 0;

    if (block->length > memory->size - sizeof(struct ReliableBlock))
      continue;

    // Order-independent sum over (identifier, length, data)
    value1   = GetCRC32C(block->identifier, sizeof(uuid_t), 0);
    value1   = GetCRC32C((const uint8_t*)&block->length, sizeof(uint32_t), value1);
    value2   = GetCRC32C(block->data, block->length, value1);
    *digest += ((uint64_t)value1 << 32) | value2;
    *count  += 1;

    payload = (struct Payload*)block->data;
    *own   += (block->length >= sizeof(struct Payload)) && (uuid_compare(payload->author, context->identifier) == 0);
  }

  pthread_rwlock_unlock(&context->pool->lock);
}

static void DumpBlocks(struct Context* context, const char* path)
{
  struct ReliableMemory* memory;
  struct ReliableBlock* block;
  struct Payload* payload;
  uint32_t number;
  uint32_t index;
  char buffer[2][40];
  FILE* file;

  if ((path == NULL) ||
      ((file = fopen(path, "w")) == NULL))
    return;

  pthread_rwlock_rdlock(&context->pool->lock);

  memory = context->pool->share->memory;

  for (number = 0; number < atomic_load_explicit(&memory->length, memory_order_relaxed); ++ number)
  {
    block   = (struct ReliableBlock*)(memory->data + (size_t)memory->size * (size_t)number);
    payload = (struct Payload*)block->data;

    if (atomic_load_explicit(&block->type, memory_order_relaxed) == RELIABLE_TYPE_FREE)
      continue;

    uuid_unparse_lower(block->identifier, buffer[0]);

    if (atomic_load_explicit(&block->hint, memory_order_relaxed) & 1ULL)
    {
      // A pending hint outside the barrier marks a copy damaged by a transfer, only its restored identifier can be trusted
      fprintf(file, "%s - - - - # %u %u %u 1\n", buffer[0], number, block->type, block->count);
      continue;
    }

    if (block->length > memory->size - sizeof(struct ReliableBlock))
      continue;

    if (block->length >= sizeof(struct Payload))
    {
      uuid_unparse_lower(payload->author, buffer[1]);
      fprintf(file, "%s %u %08x %s %llu # %u %u %u 0\n", buffer[0], block->length, GetCRC32C(block->data, block->length, 0), buffer[1], (unsigned long long)payload->sequence, number, block->type, block->count);
      continue;
    }

    fprintf(file, "%s %u %08x - - # %u %u %u 0\n", buffer[0], block->length, GetCRC32C(block->data, block->length, 0), number, block->type, block->count);
  }

  pthread_rwlock_unlock(&context->pool->lock);

  for (index = 0; index < context->released.count; ++ index)
  {
    uuid_unparse_lower(context->released.data[index], buffer[0]);
    fprintf(file, "released %s\n", buffer[0]);
  }

  // Compare.py checks that the last message of every author has arrived, a lost tail leaves no gap to count
  fprintf(file, "sent %016llx %llu\n", (unsigned long long)context->incarnation, (unsigned long long)context->posted);

  for (index = 0; (index < STREAM_COUNT) && !uuid_is_null(context->streams[index].author); ++ index)
  {
    uuid_unparse_lower(context->streams[index].author, buffer[0]);
    fprintf(file, "received %s %016llx %llu\n", buffer[0], (unsigned long long)context->streams[index].incarnation, (unsigned long long)context->streams[index].sequence);
  }

  fclose(file);
}

static uint64_t TakeCounter(ATOMIC(uint64_t)* counter, int reset)
{
  // Exchange keeps events counted between the read and the reset in the next period
  return reset ? atomic_exchange_explicit(counter, 0, memory_order_relaxed) : atomic_load_explicit(counter, memory_order_relaxed);
}

static void PrintCounters(const char* label, struct Context* context, struct Counters* counters, struct Samples* samples, int reset)
{
  uint64_t values[17];
  int64_t latencies[4];
  uint32_t blocks;
  uint32_t own;
  uint32_t stamped;
  uint32_t damaged;
  uint64_t digest;
  uint32_t delay;
  uint32_t state;
  uint32_t tasks;
  uint32_t buffers;
  struct timespec time;
  struct InstantRemoval* removal;

  values[0] = TakeCounter(&counters->writes,      reset);
  values[1] = TakeCounter(&counters->frees,       reset);
  values[2] = TakeCounter(&counters->arrivals,    reset);
  values[3] = TakeCounter(&counters->removals,    reset);
  values[4] = TakeCounter(&counters->damages,     reset);
  values[5] = TakeCounter(&counters->corrupts,    reset);
  values[6] = TakeCounter(&counters->stales,      reset);
  values[7] = TakeCounter(&counters->repeats,     reset);
  values[8] = TakeCounter(&counters->connects,    reset);
  values[9] = TakeCounter(&counters->disconnects, reset);
  values[10] = TakeCounter(&counters->messages,   reset);
  values[11] = TakeCounter(&counters->refused,    reset);
  values[12] = TakeCounter(&counters->received,   reset);
  values[13] = TakeCounter(&counters->skipped,    reset);
  values[14] = TakeCounter(&counters->disorders,  reset);
  values[15] = TakeCounter(&counters->waited,     reset);
  values[16] = TakeCounter(&counters->lost,       reset);

  SummarizeSamples(samples, latencies, reset);
  ComputeDigest(context, &blocks, &own, &stamped, &damaged, &digest);
  clock_gettime(CLOCK_MONOTONIC, &time);

  // Replicator internals are read without its lock, the values are for diagnostics only
  state   = atomic_load_explicit(&context->replicator->state, memory_order_relaxed);
  tasks   = context->replicator->schedule.count;
  buffers = atomic_load_explicit(&context->replicator->buffers.count, memory_order_relaxed);
  removal = context->replicator->removals.head;
  delay   = (removal != NULL) ? (context->replicator->tick - removal->expiration) : 0;

  printf(
    "%s t=%.1f writes=%llu frees=%llu arrivals=%llu removals=%llu damages=%llu corrupts=%llu stales=%llu repeats=%llu "
    "connects=%llu disconnects=%llu latency_us(min=%lld p50=%lld p99=%lld max=%lld) blocks=%u own=%u stamped=%u damaged=%u digest=%016llx state=%x tasks=%u buffers=%u removal_delay=%d "
    "messages=%llu refused=%llu received=%llu skipped=%llu lost=%llu disorders=%llu wait_max_ms=%.1f\n",
    label, GetElapsedTime(&context->start, &time) / 1e9,
    (unsigned long long)values[0], (unsigned long long)values[1], (unsigned long long)values[2], (unsigned long long)values[3],
    (unsigned long long)values[4], (unsigned long long)values[5], (unsigned long long)values[6], (unsigned long long)values[7],
    (unsigned long long)values[8], (unsigned long long)values[9],
    (long long)latencies[0], (long long)latencies[1], (long long)latencies[2], (long long)latencies[3],
    blocks, own, stamped, damaged, (unsigned long long)digest, state, tasks, buffers, (int32_t)delay,
    (unsigned long long)values[10], (unsigned long long)values[11], (unsigned long long)values[12], (unsigned long long)values[13],
    (unsigned long long)values[16], (unsigned long long)values[14], values[15] / 1e6);

  fflush(stdout);
}

static void HandleReportTimeout(struct FastRingDescriptor* descriptor)
{
  struct Context* context;

  context = (struct Context*)descriptor->closure;

  PrintCounters("STAT", context, &context->period, &context->recent, 1);
}

static int RegisterPeer(struct InstantReplicator* replicator, const char* specification, uint16_t port)
{
  struct addrinfo* information;
  struct addrinfo hint;
  uuid_t identifier;
  char* buffer;
  char* address;
  char* service;
  char* last;
  int result;

  // NAME@IPV4[:PORT] or NAME@[IPV6][:PORT]

  buffer = strdup(specification);

  if ((address = strchr(buffer, '@')) == NULL)
  {
    free(buffer);
    return -1;
  }

  *(address ++) = '\0';
  service       = NULL;

  if (*address == '[')
  {
    address ++;

    if (last = strchr(address, ']'))
    {
      *(last ++) = '\0';
      service    = (*last == ':') ? (last + 1) : NULL;
    }
  }
  else if (last = strrchr(address, ':'))
  {
    *(last ++) = '\0';
    service    = last;
  }

  if (uuid_parse(buffer, identifier) != 0)
    uuid_generate_sha1(identifier, *uuid_get_template("oid"), buffer, strlen(buffer));

  memset(&hint, 0, sizeof(struct addrinfo));
  hint.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;
  hint.ai_socktype = SOCK_STREAM;

  information = NULL;
  result      = -1;

  if (service == NULL)
  {
    asprintf(&service, "%u", port);
    last = service;
  }
  else
    last = NULL;

  if (getaddrinfo(address, service, &hint, &information) == 0)
    result = RegisterRemoteInstantReplicator(replicator, identifier, information->ai_addr, information->ai_addrlen);

  freeaddrinfo(information);
  free(last);
  free(buffer);

  return result;
}

static void PrintUsage(const char* name)
{
  printf(
    "Usage: %s -n NAME [options]\n"
    "  -n NAME          node name or UUID (identifier is derived from the name)\n"
    "  -l PORT          RDMA CM listen port (default 7400)\n"
    "  -p NAME@ADDRESS  peer, ADDRESS is IPV4[:PORT] or [IPV6][:PORT], repeatable; no peers = avahi discovery\n"
    "  -r RATE          operations per second (default 100)\n"
    "  -k COUNT         number of own block slots (default 4096)\n"
    "  -s SIZE          maximum payload size in bytes (default 256)\n"
    "  -f PERCENT       free probability of an existing slot (default 10)\n"
    "  -t SECONDS       writing duration counted from the first peer connection, 0 = until SIGINT (default 0)\n"
    "  -q SECONDS       quiescence before the final digest, removals apply after 10 s (default 15)\n"
    "  -S SECRET        replicator secret (default Secret)\n"
    "  -u RATE          user messages per second, sent by the application thread with wait (default 0)\n"
    "  -e MS            time a peer may go without progress before its connection is closed, 0 = default 1000 ms\n"
    "  -O               optimistic mode: RDMA READ under the receiver barrier, synchronous transfer as a fallback;\n"
    "                   DAMAGE is expected there, Compare.py judges the blocks left damaged\n"
    "  -o FILE          dump surviving blocks (identifier, length, CRC32C, author, sequence # number, type, count, damaged)\n"
    "                   and the identifiers of released own blocks (released IDENTIFIER) at exit\n"
    "SIGUSR1 prints totals with the current digest\n"
    "Exit status: 0 = passed, 1 = setup or runtime failure, 2 = verification failure (corrupt or stale arrivals,\n"
    "DAMAGE without -O and without a disconnect, user messages out of order or lost within a connection,\n"
    "no peer connected)\n",
    name);
}

int main(int count, char** arguments)
{
  struct sigaction action;
  struct Context context;
  const char* name;
  const char* secret;
  const char* path;
  uint32_t options;
  uint32_t timeout;
  char buffer[40];
  uint16_t port;
  int option;
  int status;
  int failure;
  int result;

  int handle;
  struct ReliableMonitor monitor;
  struct ReliableTracker* tracker;
  struct ReliableIndexer* indexer;
  struct InstantReplicator* replicator;
  struct InstantDiscovery* discovery;

  struct FastRing* ring;
  struct FastRingDescriptor* waiter1;
  struct FastRingDescriptor* waiter2;
  struct FastRingDescriptor* writer;
  struct FastRingDescriptor* reporter;
  struct timespec time;

  AvahiPoll* poll;

  memset(&context, 0, sizeof(struct Context));

  context.rate       = 100;
  context.count      = 4096;
  context.size       = 256;
  context.ratio      = 10;
  context.quiescence = 15;

  getrandom(&context.incarnation, sizeof(context.incarnation), 0);

  name    = NULL;
  path    = NULL;
  secret  = "Secret";
  port    = 7400;
  options = 0;
  timeout = 0;

  while ((option = getopt(count, arguments, "n:l:p:r:k:s:f:t:q:S:o:u:e:Oh")) != -1)
  {
    switch (option)
    {
      case 'n':  name               = optarg;        break;
      case 'l':  port               = atoi(optarg);  break;
      case 'p':  context.peers     ++;               break;
      case 'r':  context.rate       = atoi(optarg);  break;
      case 'k':  context.count      = atoi(optarg);  break;
      case 's':  context.size       = atoi(optarg);  break;
      case 'f':  context.ratio      = atoi(optarg);  break;
      case 't':  context.duration   = atoi(optarg);  break;
      case 'q':  context.quiescence = atoi(optarg);  break;
      case 'S':  secret             = optarg;        break;
      case 'o':  path               = optarg;        break;
      case 'u':  context.messages   = atoi(optarg);  break;
      case 'O':  options           |= INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE;  break;
      case 'e':  timeout            = atoi(optarg);  break;

      default:
        PrintUsage(arguments[0]);
        return 1;
    }
  }

  if ((name == NULL) ||
      (context.count == 0) ||
      (context.size < sizeof(struct Payload)))
  {
    PrintUsage(arguments[0]);
    return 1;
  }

  if (uuid_parse(name, context.identifier) != 0)
    uuid_generate_sha1(context.identifier, *uuid_get_template("oid"), name, strlen(name));

  // Events are printed from the replicator thread too, keep the log complete when the process hangs or is killed
  setvbuf(stdout, NULL, _IOLBF, 0);

  memset(&action, 0, sizeof(struct sigaction));
  action.sa_handler = HandleSignal;
  action.sa_flags   = SA_NODEFER | SA_RESTART;

  sigemptyset(&action.sa_mask);
  sigaction(SIGHUP,  &action, NULL);
  sigaction(SIGINT,  &action, NULL);
  sigaction(SIGTERM, &action, NULL);
  sigaction(SIGQUIT, &action, NULL);
  sigaction(SIGUSR1, &action, NULL);

  context.descriptors = (struct ReliableDescriptor*)calloc(context.count, sizeof(struct ReliableDescriptor));
  context.history     = (struct History*)calloc(HISTORY_LENGTH, sizeof(struct History));

  InitializeSamples(&context.overall, SAMPLE_LIMIT);
  InitializeSamples(&context.recent,  SAMPLE_LIMIT);

  if ((context.descriptors  == NULL) ||
      (context.history      == NULL) ||
      (context.overall.data == NULL) ||
      (context.recent.data  == NULL))
  {
    printf("Failed to allocate test state\n");
    free(context.descriptors);
    free(context.history);
    free(context.overall.data);
    free(context.recent.data);
    return 1;
  }

  memset(&monitor, 0, sizeof(struct ReliableMonitor));

  monitor.function = HandleMonitorEvent;
  monitor.closure  = &context;

  // Every stage is created only on top of the previous one, the cleanup below accepts NULL for any of them
  poll         = NULL;
  discovery    = NULL;
  waiter1      = NULL;
  waiter2      = NULL;
  writer       = NULL;
  reporter     = NULL;
  result       = 1;
  handle       = memfd_create(POOL_NAME, MFD_CLOEXEC);
  replicator   = (handle >= 0) ? CreateInstantReplicator(port, context.identifier, SERVICE_NAME, secret, options, timeout, HandleReplicatorEvent, &context, &monitor) : NULL;
  indexer      = (replicator != NULL) ? CreateReliableIndexer(&replicator->super) : NULL;
  tracker      = (indexer    != NULL) ? CreateReliableTracker(RELIABLE_TRACKER_FLAG_ID_HOST | RELIABLE_TRACKER_FLAG_ID_PROCESS, &indexer->super) : NULL;
  context.pool = (tracker    != NULL) ? CreateReliablePool(handle, POOL_NAME, context.size, 0, &tracker->super, NULL, NULL) : NULL;
  ring         = (context.pool != NULL) ? CreateFastRing(0) : NULL;

  context.replicator = replicator;

  uuid_unparse_lower(context.identifier, buffer);
  printf("Node %s (%s), port %u, rate %u/s, slots %u, size %u, free %u%%\n", name, buffer, port, context.rate, context.count, context.size, context.ratio);

  if (ring == NULL)
  {
    printf("Failed to create %s\n", (handle < 0) ? "memfd" : (replicator == NULL) ? "replicator" : (indexer == NULL) ? "indexer" : (tracker == NULL) ? "tracker" : (context.pool == NULL) ? "pool" : "ring");
  }
  else if (~atomic_load_explicit(&tracker->state, memory_order_relaxed) & RELIABLE_TRACKER_STATE_ACTIVE)
  {
    printf("ReliableTracker is not active (userfaultfd requires CAP_SYS_PTRACE)\n");
  }
  else if (~atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_ACTIVE)
  {
    printf("Failed to open RDMA port\n");
  }
  else if (!(waiter1 = SubmitReliableWaiter(ring, tracker)) ||
           !(waiter2 = SubmitInstantWaiter(ring, replicator)))
  {
    printf("Failed to submit waiters (IORING_OP_FUTEX_WAIT is required)\n");
  }
  else
  {
    // Setup is complete
    result = 0;
  }

  optind = 1;

  while ((result == 0) &&
         ((option = getopt(count, arguments, "n:l:p:r:k:s:f:t:q:S:o:u:e:Oh")) != -1))
  {
    if ((option == 'p') &&
        (RegisterPeer(replicator, optarg, port) != 0))
    {
      printf("Invalid peer %s\n", optarg);
      result = 1;
    }
  }

  if ((result == 0) &&
      (context.peers == 0))
  {
    poll      = CreateFastAvahiPoll(ring);
    discovery = CreateInstantDiscovery(poll, replicator);

    if (discovery == NULL)
    {
      printf("Failed to create avahi-client\n");
      result = 1;
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &context.launch);
  context.start = context.launch;

  if (result == 0)
  {
    writer   = SetFastRingTimeout(ring, NULL, 1,    TIMEOUT_FLAG_REPEAT, HandleWriteTimeout,  &context);
    reporter = SetFastRingTimeout(ring, NULL, 1000, TIMEOUT_FLAG_REPEAT, HandleReportTimeout, &context);
    result   = ((writer == NULL) || (reporter == NULL));
    printf(result ? "Failed to set timers\n" : "Started\n");
  }

  fflush(stdout);

  while (result == 0)
  {
    if (((status = WaitForFastRing(ring, 200, NULL)) < 0) &&
        (status != -EINTR))
    {
      // A stop and continue of the process (SIGSTOP / SIGCONT in failure runs) interrupts the wait
      printf("FAILED: WaitForFastRing() returned %d\n", status);
      result = 1;
      break;
    }

    clock_gettime(CLOCK_MONOTONIC, &time);

    if (atomic_exchange_explicit(&requested, 0, memory_order_relaxed))
      PrintCounters("TOTAL", &context, &context.total, &context.overall, 0);

    if ((context.stop.tv_sec == 0) &&
        (context.duration != 0) &&
        (atomic_load_explicit(&context.total.connects, memory_order_relaxed) == 0) &&
        (GetElapsedTime(&context.launch, &time) >= context.duration * 1000000000LL))
    {
      // No peer within the whole duration, the verification below reports it
      break;
    }

    if ((context.stop.tv_sec == 0) &&
        (atomic_load_explicit(&signaled, memory_order_relaxed) ||
         (context.duration != 0) && (GetElapsedTime(&context.start, &time) >= context.duration * 1000000000LL)))
    {
      atomic_store_explicit(&signaled, 0, memory_order_relaxed);
      context.stop = time;
      printf("Quiescing for %u seconds\n", context.quiescence);
      fflush(stdout);
    }

    if ((context.stop.tv_sec != 0) &&
        (atomic_load_explicit(&signaled, memory_order_relaxed) ||
         (GetElapsedTime(&context.stop, &time) >= context.quiescence * 1000000000LL)))
      break;
  }

  if (ring != NULL)
  {
    FlushReliableTracker(tracker);
    PrintCounters("FINAL", &context, &context.total, &context.overall, 0);
    DumpBlocks(&context, path);
  }

  // Runtime failure flags are taken here, the objects are released below
  failure = (ring != NULL) &&
            ((atomic_load_explicit(&tracker->state,    memory_order_relaxed) & RELIABLE_TRACKER_STATE_FAILURE) ||
             (atomic_load_explicit(&replicator->state, memory_order_relaxed) & INSTANT_REPLICATOR_STATE_FAILURE));

  SetFastRingTimeout(ring, reporter, -1, 0, NULL, NULL);
  SetFastRingTimeout(ring, writer,   -1, 0, NULL, NULL);
  ReleaseInstantDiscovery(discovery);
  CancelInstantWaiter(waiter2);
  CancelReliableWaiter(waiter1);
  ReleaseFastAvahiPoll(poll);
  ReleaseFastRing(ring);

  // Own blocks are not released on purpose: ReleaseReliableBlock() of any type sends INSTANT_TYPE_REMOVE,
  // and peers that finish later would lose these blocks before taking their dumps.
  // The held references keep the pool mapped until the process exits.
  ReleaseReliablePool(context.pool);
  // The replicator thread uses the indexer until it is joined, so the replicator goes first
  ReleaseInstantReplicator(replicator);
  ReleaseReliableTracker(tracker);
  ReleaseReliableIndexer(indexer);

  if (handle >= 0)
    close(handle);

  // The replicator thread is joined, no event can change the totals anymore
  if (result == 0)
  {
    if (failure)
    {
      printf("FAILED: tracker or replicator reported a runtime failure\n");
      result = 1;
    }
    else if (atomic_load_explicit(&context.total.corrupts, memory_order_relaxed) ||
             atomic_load_explicit(&context.total.stales,   memory_order_relaxed) ||
             (atomic_load_explicit(&context.total.damages,     memory_order_relaxed) &&
              (atomic_load_explicit(&context.total.disconnects, memory_order_relaxed) == 0) &&
              (~options & INSTANT_REPLICATOR_OPTION_OPTIMISTIC_MODE)))
    {
      // A rejected optimistic read overwrites the copy, and a disconnect leaves a block locked by the sender written in part,
      // so DAMAGE is expected in these cases until a later version repairs it
      printf("FAILED: corrupt, damaged or stale arrivals\n");
      result = 2;
    }
    else if (atomic_load_explicit(&context.total.disorders, memory_order_relaxed) ||
             atomic_load_explicit(&context.total.lost,      memory_order_relaxed))
    {
      // A user message may be lost only with a broken connection to its author
      printf("FAILED: user messages out of order or lost without a disconnect\n");
      result = 2;
    }
    else if (atomic_load_explicit(&context.total.connects, memory_order_relaxed) == 0)
    {
      printf("FAILED: no peer has connected, nothing was verified\n");
      result = 2;
    }
  }

  free(context.descriptors);
  free(context.history);
  free(context.released.data);
  free(context.overall.data);
  free(context.recent.data);

  return result;
}
