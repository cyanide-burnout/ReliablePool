#include <lua5.1/lua.h>
#include <lua5.1/lualib.h>
#include <lua5.1/lauxlib.h>

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "ReliablePool.h"

struct RecoveryContext
{
  lua_State* state;
  int function;
};

static struct ReliablePool* GetPool(lua_State* state)
{
  struct ReliablePool** pointer;

  pointer = (struct ReliablePool**)luaL_checkudata(state, 1, "ReliablePool");

  if ((pointer  == NULL) ||
      (*pointer == NULL))
    luaL_error(state, "pool is closed");

  return *pointer;
}

static struct ReliableDescriptor* GetDescriptor(lua_State* state)
{
  struct ReliableDescriptor* descriptor;

  descriptor = (struct ReliableDescriptor*)luaL_checkudata(state, 1, "ReliableBlock");

  if ((descriptor->pool  == NULL) ||
      (descriptor->share == NULL) ||
      (descriptor->block == NULL))
    luaL_error(state, "block is released");

  return descriptor;
}

static size_t GetBlockCapacity(struct ReliableDescriptor* descriptor)
{
  struct ReliableShare* share;
  struct ReliableMemory* memory;

  share  = descriptor->share;
  memory = share->memory;

  return memory->size - offsetof(struct ReliableBlock, data);
}

static struct ReliableDescriptor* PushRecoveredDescriptor(lua_State* state, struct ReliablePool* pool, struct ReliableBlock* block)
{
  struct ReliableDescriptor* descriptor;

  descriptor = (struct ReliableDescriptor*)lua_newuserdata(state, sizeof(struct ReliableDescriptor));

  RecoverReliableBlock(descriptor, pool, block);

  luaL_getmetatable(state, "ReliableBlock");
  lua_setmetatable(state, -2);

  return descriptor;
}

static int InvokeRecoveryFunction(struct ReliablePool* pool, struct ReliableBlock* block, void* closure)
{
  struct RecoveryContext* context;
  lua_State* state;

  context = (struct RecoveryContext*)closure;
  state   = context->state;

  lua_rawgeti(state, LUA_REGISTRYINDEX, context->function);
  PushRecoveredDescriptor(state, pool, block);

  if (lua_pcall(state, 1, 0, 0) != 0)
    lua_pop(state, 1);

  return RELIABLE_TYPE_RECOVERABLE;
}

static int ReleasePool(lua_State* state)
{
  struct ReliablePool** pointer;

  pointer = (struct ReliablePool**)luaL_checkudata(state, 1, "ReliablePool");

  if (*pointer != NULL)
  {
    ReleaseReliablePool(*pointer);
    *pointer = NULL;
  }

  return 0;
}

static int AllocateBlock(lua_State* state)
{
  struct ReliableDescriptor* descriptor;
  struct ReliablePool* pool;
  int type;

  pool       = GetPool(state);
  type       = luaL_optinteger(state, 2, RELIABLE_TYPE_NON_RECOVERABLE);
  descriptor = (struct ReliableDescriptor*)lua_newuserdata(state, sizeof(struct ReliableDescriptor));

  if (AllocateReliableBlock(descriptor, pool, type) == NULL)
    return luaL_error(state, "failed to allocate block");

  luaL_getmetatable(state, "ReliableBlock");
  lua_setmetatable(state, -2);
  return 1;
}

static int AttachBlock(lua_State* state)
{
  struct ReliableDescriptor* descriptor;
  struct ReliablePool* pool;
  lua_Integer number;
  lua_Integer tag;

  pool   = GetPool(state);
  number = luaL_checkinteger(state, 2);
  tag    = lua_isnoneornil(state, 3) ? UINT32_MAX : luaL_checkinteger(state, 3);

  if (number < 0)                  return luaL_error(state, "block number must be non-negative");
  if (tag    < 0)                  return luaL_error(state, "block tag must be non-negative");
  if (!lua_isnoneornil(state, 4))  return luaL_error(state, "argument #4 is not supported for attach");

  descriptor = (struct ReliableDescriptor*)lua_newuserdata(state, sizeof(struct ReliableDescriptor));

  if (AttachReliableBlock(descriptor, pool, number, tag) == NULL)
    return luaL_error(state, "failed to attach block");

  luaL_getmetatable(state, "ReliableBlock");
  lua_setmetatable(state, -2);
  return 1;
}

static int UpdatePool(lua_State* state)
{
  struct ReliablePool* pool;
  int result;

  pool   = GetPool(state);
  result = UpdateReliablePool(pool);

  if (result < 0)
    return luaL_error(state, "failed to update pool: %s", strerror(-result));

  lua_pushinteger(state, result);
  return 1;
}

static int GetPoolIndex(lua_State* state)
{
  const char* key;

  luaL_checkudata(state, 1, "ReliablePool");

  key = luaL_checkstring(state, 2);

  if (strcmp(key, "allocate") == 0)  {  lua_pushcfunction(state, AllocateBlock);  return 1;  }
  if (strcmp(key, "attach")   == 0)  {  lua_pushcfunction(state, AttachBlock);    return 1;  }
  if (strcmp(key, "update")   == 0)  {  lua_pushcfunction(state, UpdatePool);     return 1;  }
  if (strcmp(key, "close")    == 0)  {  lua_pushcfunction(state, ReleasePool);    return 1;  }

  lua_pushnil(state);
  return 1;
}

static int ReleaseBlock(lua_State* state)
{
  struct ReliableDescriptor* descriptor;
  int type;

  descriptor = (struct ReliableDescriptor*)luaL_checkudata(state, 1, "ReliableBlock");
  type       = lua_isnoneornil(state, 2) ? RELIABLE_TYPE_FREE : luaL_checkinteger(state, 2);

  if (descriptor->pool != NULL)
    ReleaseReliableBlock(descriptor, type);

  return 0;
}

static int GetBlockIndex(lua_State* state)
{
  struct ReliableDescriptor* descriptor;
  struct ReliableBlock* block;
  char identifier[37];
  const char* key;

  descriptor = GetDescriptor(state);
  key        = luaL_checkstring(state, 2);

  if (strcmp(key, "release") == 0)
  {
    lua_pushcfunction(state, ReleaseBlock);
    return 1;
  }

  block = descriptor->block;

  if (strcmp(key, "data")   == 0)  {  lua_pushlstring(state, (const char*)block->data, block->length);  return 1;  }
  if (strcmp(key, "type")   == 0)  {  lua_pushinteger(state, block->type);    return 1;  }
  if (strcmp(key, "number") == 0)  {  lua_pushinteger(state, block->number);  return 1;  }
  if (strcmp(key, "length") == 0)  {  lua_pushinteger(state, block->length);  return 1;  }
  if (strcmp(key, "tag")    == 0)  {  lua_pushnumber(state, (lua_Number)atomic_load_explicit(&block->tag,   memory_order_relaxed));  return 1;  }
  if (strcmp(key, "mark")   == 0)  {  lua_pushnumber(state, (lua_Number)atomic_load_explicit(&block->mark,  memory_order_relaxed));  return 1;  }
  if (strcmp(key, "count")  == 0)  {  lua_pushnumber(state, (lua_Number)atomic_load_explicit(&block->count, memory_order_relaxed));  return 1;  }

  if (strcmp(key, "identifier") == 0)
  {
    uuid_unparse_lower(block->identifier, identifier);
    lua_pushstring(state, identifier);
    return 1;
  }

  lua_pushnil(state);
  return 1;
}

static int SetBlockIndex(lua_State* state)
{
  struct ReliableDescriptor* descriptor;
  struct ReliableBlock* block;
  const char* data;
  const char* key;
  size_t capacity;
  size_t length;

  descriptor = GetDescriptor(state);

  block    = descriptor->block;
  key      = luaL_checkstring(state, 2);
  capacity = GetBlockCapacity(descriptor);

  if (strcmp(key, "length") == 0)
  {
    length = luaL_checkinteger(state, 3);
    if (length > capacity)  return luaL_error(state, "length exceeds block capacity");

    block->length = length;
    return 0;
  }

  if (strcmp(key, "data") == 0)
  {
    data = luaL_checklstring(state, 3, &length);
    if (length > capacity)  return luaL_error(state, "data exceeds block capacity");

    memcpy(block->data, data, length);
    block->length = length;
    return 0;
  }

  return luaL_error(state, "property '%s' is read-only or unknown", key);
}

static int OpenPool(lua_State* state)
{
  struct RecoveryContext context;
  struct ReliablePool** pointer;
  struct ReliablePool* pool;
  lua_Integer length;
  const char* name;
  int function;
  int handle;

  memset(&context, 0, sizeof(struct RecoveryContext));

  handle   = -1;
  name     = luaL_checkstring(state, 2);
  length   = luaL_checkinteger(state, 3);
  function = 0;

  if (lua_gettop(state) > 4)    return luaL_error(state, "too many arguments");
  if (length <= 0)              return luaL_error(state, "length must be greater than zero");

  if (lua_gettop(state) >= 4)
  {
    if (lua_isfunction(state, 4))   function = 4;
    else if (!lua_isnil(state, 4))  return luaL_error(state, "argument #4 must be recover function");
  }

  if (lua_type(state, 1)      == LUA_TNUMBER)  handle = (int)lua_tointeger(state, 1);
  else if (lua_type(state, 1) == LUA_TSTRING)  handle = open(lua_tostring(state, 1), O_RDWR | O_CREAT, 0660);
  else                                         return luaL_error(state, "argument #1 must be path or file descriptor");
  if (handle < 0)                              return luaL_error(state, "failed to open pool: %s", strerror(errno));

  if (function != 0)
  {
    lua_pushvalue(state, function);
    context.state    = state;
    context.function = luaL_ref(state, LUA_REGISTRYINDEX);
    pool             = CreateReliablePool(handle, name, length, RELIABLE_FLAG_RESET, NULL, InvokeRecoveryFunction, &context);
    luaL_unref(state, LUA_REGISTRYINDEX, context.function);
  }
  else
  {
    // Open without recovery
    pool = CreateReliablePool(handle, name, length, 0, NULL, NULL, NULL);
  }

  if (pool == NULL)
  {
    close(handle);
    return luaL_error(state, "failed to create pool");
  }

  pointer  = (struct ReliablePool**)lua_newuserdata(state, sizeof(struct ReliablePool*));
  *pointer = pool;

  luaL_getmetatable(state, "ReliablePool");
  lua_setmetatable(state, -2);

  return 1;
}

LUA_API int luaopen_ReliablePool(lua_State* state)
{
  luaL_newmetatable(state, "ReliablePool");
  lua_pushliteral(state, "__gc");     lua_pushcfunction(state, ReleasePool);   lua_settable(state, -3);
  lua_pushliteral(state, "__index");  lua_pushcfunction(state, GetPoolIndex);  lua_settable(state, -3);
  lua_pop(state, 1);

  luaL_newmetatable(state, "ReliableBlock");
  lua_pushliteral(state, "__gc");        lua_pushcfunction(state, ReleaseBlock);   lua_settable(state, -3);
  lua_pushliteral(state, "__index");     lua_pushcfunction(state, GetBlockIndex);  lua_settable(state, -3);
  lua_pushliteral(state, "__newindex");  lua_pushcfunction(state, SetBlockIndex);  lua_settable(state, -3);
  lua_pop(state, 1);

  lua_newtable(state);
  lua_pushliteral(state, "open");                           lua_pushcfunction(state, OpenPool);                     lua_settable(state, -3);
  lua_pushliteral(state, "RELIABLE_TYPE_FREE");             lua_pushinteger(state, RELIABLE_TYPE_FREE);             lua_settable(state, -3);
  lua_pushliteral(state, "RELIABLE_TYPE_RECOVERABLE");      lua_pushinteger(state, RELIABLE_TYPE_RECOVERABLE);      lua_settable(state, -3);
  lua_pushliteral(state, "RELIABLE_TYPE_NON_RECOVERABLE");  lua_pushinteger(state, RELIABLE_TYPE_NON_RECOVERABLE);  lua_settable(state, -3);

  return 1;
}
