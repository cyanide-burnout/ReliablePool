#ifndef CRC32C_H
#define CRC32C_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef uint32_t (*HashFunction)(uint32_t value);
typedef uint32_t (*CRC32CFunction)(const uint8_t* data, size_t length, uint32_t value);

extern HashFunction GetHash;
extern CRC32CFunction GetCRC32C;

#ifdef __cplusplus
}
#endif

#endif