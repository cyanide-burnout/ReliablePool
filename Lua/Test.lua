#!/usr/bin/env luajit

local module = require("ReliablePool")

local records = {}

local function RecoverRecord(block)
  records[#records + 1] = block
end

local pool = module.open("test.dat", "Test", 128, RecoverRecord)

print(string.format("Recovered records: %d", #records))

for number = 1, 10 do
  local block = pool:allocate(module.RELIABLE_TYPE_RECOVERABLE)
  block.data = string.format("record=%d time=%d", number, os.time())
  records[#records + 1] = block
end

for _, block in ipairs(records) do
  print(block.data)
  block:release(module.RELIABLE_TYPE_RECOVERABLE)
end

pool:close()
