--[[
    This is example of the use avro lua module
]]--

package.path = './?/init.lua;'..package.path

local ffi = require('ffi')
local avro = require('avro')
local cjson = require('cjson')


local sch = {type = "record", name = "test", fields = {{name = "a", type = "int"}, {name = "b", type = "string"}}}
local data = {{101, "helllo and"}, {20, "GET /Hello"}}

local s = cjson.encode(sch)

-- Buffer with serialized avro data
local buf = ffi.new('uint8_t[4096]')

-- Data count in biffer
local count = 0

print ('Start encode')
count, str = avro.encode(sch, data, buf, 1024)
print ('---------------------------------')
print ('Avro byte string')
print (str)

print ('Data count')
print (count)

print ('---------------------------------')
print ('Start decode')
local res = avro.decode(sch, buf, count)

print ('---------------------------------')
for k, v in pairs(res) do
    for n, d in pairs(v) do
        print (n..' = '.. d)
    end
end
