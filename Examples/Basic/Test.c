#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>

#include "ReliablePool.h"

int recover(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  char* buffer;
  struct ReliableDescriptor desc;

  buffer = (char*)RecoverReliableBlock(&desc, pool, block);

  printf("Recover block: %s\n", buffer);
  *((int*)closure) += 1;

  return RELIABLE_TYPE_RECOVERABLE;
}

int main(int count, char** arguments)
{
  int handle;
  int number;
  char* buffer;
  struct ReliablePool* pool;
  struct ReliableDescriptor desc[10000];

  number = 0;
  handle = open("test.dat", O_RDWR | O_CREAT, 0666);
  pool   = CreateReliablePool(handle, "Test", 100, RELIABLE_FLAG_RESET, NULL, recover, &number);

  if (pool == NULL)
  {
    printf("Error opening pool\n");
    return 1;
  }

  printf("Pool size=%lld, recovered=%d\n", pool->share->memory->length, number);

  number = 0;
  while (number < 400)
  {
  	buffer = (char*)AllocateReliableBlock(desc + number, pool, RELIABLE_TYPE_RECOVERABLE);
  	sprintf(buffer, "data=%d pid=%d", number, getpid());

  	// printf("number=%d share=%p\n", number, desc[number].share);
  	number ++;
  }

  number = 0;
  while (number < 10)
  {
  	ReleaseReliableBlock(desc + number, RELIABLE_TYPE_FREE);
  	number ++;
  }

  ReleaseReliablePool(pool);
  fsync(handle);
  close(handle);
  return 0;
}