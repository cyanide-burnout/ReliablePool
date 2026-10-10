# Lua Module

`Lua/Module.c` is a Lua 5.1 / LuaJIT binding of the core pool (`ReliablePool` and
`VerifyReliableBlockIntegrity()` of `ReliableTracker`). It does not expose monitors, the tracker
thread or replication.

## Build and Run

```bash
make -C Lua
```

```bash
cd Lua && ./Test.lua
```

The build produces `ReliablePool.so` and depends on `uuid`, `lua5.1` and `libsystemd` through
`pkg-config`.

## API

```lua
local module = require("ReliablePool")

pool  = module.open(path_or_fd, name, length[, recover])
block = pool:allocate([type])
block = pool:attach(number[, tag])
result = pool:update()
pool:close()

block:release([type])
valid = block:verify()
```

Constants: `module.RELIABLE_TYPE_FREE`, `module.RELIABLE_TYPE_RECOVERABLE`,
`module.RELIABLE_TYPE_NON_RECOVERABLE`.

### Opening

- A string `path_or_fd` is opened with `O_RDWR | O_CREAT` and mode `0660`; a number is used as a
  file descriptor.
- The pool owns the descriptor and closes it on `pool:close()` or garbage collection.
- With a `recover` function the pool is opened with `RELIABLE_FLAG_RESET`; without it the pool is
  opened as is (flags 0), which is how a second process maps a pool in use.

### Recovery

- Signature: `recover(block)`, called for every recoverable block.
- The block is passed with a reference already taken (`RecoverReliableBlock()`). Keep it in a
  table to keep the object; a block that is garbage collected is released as
  `RELIABLE_TYPE_FREE`.
- Use `block:verify()` to check the CRC32C stored by `ReliableTracker` before keeping a block.
- The return value is ignored: the C function always returns `RELIABLE_TYPE_RECOVERABLE`.
- Errors raised by the function are swallowed and do not abort `open`.

### Defaults

- `pool:allocate()` allocates `RELIABLE_TYPE_NON_RECOVERABLE`.
- `pool:attach(number)` without `tag` accepts any generation of the block.
- `block:release()` and garbage collection release with `RELIABLE_TYPE_FREE`.

### Properties

| Object | Read-only | Read/write |
|---|---|---|
| pool | `size` (block size), `length` (number of blocks) | — |
| block | `type`, `number`, `count`, `mark`, `tag`, `identifier` | `length`, `data` (binary string) |

Writing `data` copies the string into the block and sets `length` to its size; both are limited by
the block capacity.
