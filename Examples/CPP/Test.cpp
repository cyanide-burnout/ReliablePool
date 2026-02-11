#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <list>
#include <cstdio>
#include <iostream>

#include "ReliablePool.h"

struct RecordData
{
  struct timespec time;
  char text[128];
};

class Record
{
  public:

    explicit Record(struct ReliablePool* pool) :
      data(pool, RELIABLE_TYPE_RECOVERABLE)
    {
      if (data)
      {
        clock_gettime(CLOCK_REALTIME, &data->time);
        std::snprintf(data->text, sizeof(data->text), "pid=%d sec=%lld nsec=%ld", getpid(), (long long)data->time.tv_sec, data->time.tv_nsec);
      }
    }

    Record(struct ReliablePool* pool, struct ReliableBlock* block) :
      data(pool, block)
    {
      // Recovered record from existing block.
    }

    void print()
    {
      if (data)
      {
        std::cout << data->text << "\n";
      }
    }

    void release()
    {
      data.release(RELIABLE_TYPE_RECOVERABLE);
    }

  private:

    ReliableHolder<RecordData> data;
};

int main(int count, char** arguments)
{
  std::list<Record> records;

  int handle = open("test.dat", O_RDWR | O_CREAT, 0666);

  if (handle < 0)
  {
    std::cout << "Error opening file\n";
    return 1;
  }

  struct ReliablePool* pool = CreateReliablePool(handle, "Test", sizeof(RecordData), RELIABLE_FLAG_RESET, NULL,
    [] (struct ReliablePool* pool, struct ReliableBlock* block, void* closure) -> int
    {
      auto records = static_cast<std::list<Record>*>(closure);
      records->emplace_back(pool, block);
      return RELIABLE_TYPE_RECOVERABLE;
    }, &records);

  if (pool == NULL)
  {
    std::cout << "Error opening pool\n";
    close(handle);
    return 1;
  }

  std::cout << "Recovered records: " << records.size() << "\n";

  for (int number = 0; number < 10; ++number)
  {
    records.emplace_back(pool);
  }

  for (auto& record : records)
  {
    record.print();
    record.release();
  }

  ReleaseReliablePool(pool);
  fsync(handle);
  close(handle);
  return 0;
}
