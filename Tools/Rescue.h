#ifndef RESCUE_H
#define RESCUE_H

#ifdef __cplusplus
extern "C"
{
#endif

// Special names: "unknown", "stored", "connection" (don't use ':' and control characters in the name)

#define RESCUE_SOURCE_PROCESS  0
#define RESCUE_SOURCE_SYSTEM   1

#define RESCUE_KEEP_ACQUIRE   -1
#define RESCUE_KEEP_IGNORE     0

#define RESCUE_REMOVE_SAVE     1
#define RESCUE_REMOVE_CLOSE    2

typedef int (*HandleRescuedHandle)(int handle, const char* name, int source, void* closure);

// A format without '%' is stored by pointer, not copied: pass a string that outlives the record (e.g. a literal)

void AddRescuedHandle(int handle, const char* format, ...);
void RemoveRescuedHandle(int handle, int action);

void IterateRescuedHandleList(HandleRescuedHandle function, void* closure);
int GetRescuedHandle(const char* format, ...);

int GetRescuedCount();
void CloseUnusedRescuedHandleList();

#ifdef __cplusplus
}
#endif

#endif
