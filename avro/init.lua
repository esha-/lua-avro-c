--[[
    This lua module doing encode/decode data to apache avro object.
    It using avro-c lib.
]]--
local ffi = require('ffi')
local cjson = require('cjson')

ffi.cdef [[

    typedef struct avro_schema_error_t_ *avro_schema_error_t;
    typedef struct avro_obj_t *avro_schema_t;
    typedef struct avro_value_iface  avro_value_iface_t;
    avro_value_iface_t  *writer_iface;
    typedef struct avro_value {
        avro_value_iface_t  *iface;
        void  *self;
    } avro_value_t;
    avro_value_t  writer_value;
    typedef struct avro_reader_t_ *avro_reader_t;
    typedef struct avro_writer_t_ *avro_writer_t;
    char x;

    int avro_schema_from_json(const char *jsontext, int32_t unused1,
        avro_schema_t *schema, avro_schema_error_t *unused2);
    avro_value_iface_t * avro_generic_class_from_schema(avro_schema_t schema);
    int avro_generic_value_new(avro_value_iface_t *iface, avro_value_t *dest);
    
    void avro_value_get_by_name_f(avro_value_t *dest, const char *name, avro_value_t *field);
    void avro_value_get_int_f(avro_value_t *field, int32_t *val);
    void avro_value_set_int_f(avro_value_t *field, int32_t val);

    void avro_value_set_string_f (avro_value_t *field, const char* val);
    void avro_value_get_string_f (const avro_value_t *field, const char** val, size_t* size);

    avro_writer_t avro_writer_memory(const char *buf, int64_t len);
    int avro_value_write(avro_writer_t writer, avro_value_t *src);

    avro_reader_t avro_reader_memory(const char *buf, int64_t len);
    int avro_value_read(avro_reader_t reader, avro_value_t *dest);

    void hello (char);
    int write_data(const char *, uint8_t *buf);
    void read_data(const char *, uint8_t *buf);
]]


-- Add C avro lib
local r,lb = pcall(function() return ffi.load('/usr/local/lib/libluvr.so') end)


-- Function get value from avro value object
local function value_get(name, v_type, writer_value, field)
        local a = nil
        -- We have get_data methods for diffrent data type in avro
        if v_type == 'int' then
            a = ffi.new('int32_t[?]', 1)
            lb.avro_value_get_by_name_f(writer_value, name, field)
            lb.avro_value_get_int_f(field, a)
            print ('#read: '..name..' = '..tostring(a[0]))
            return a[0]
        end
        if v_type == 'string' then
            sz = ffi.new('size_t[?]', 2)
            a = ffi.new('const char*[?]', 1)
            lb.avro_value_get_by_name_f(writer_value, name, field)
            lb.avro_value_get_string_f(field, a, sz)
            print ('#read: '..name..' = '..ffi.string(a[0]))
            return ffi.string(a[0])
        end
end

-- Function get value from avro value object
local function value_set(name, val, v_type, writer_value, field)
        -- We have get_data methods for diffrent data type in avro
        if v_type == 'int' then
            lb.avro_value_get_by_name_f(writer_value, name, field)
            lb.avro_value_set_int_f(field, val)
        end
        if v_type == 'string' then
            lb.avro_value_get_by_name_f(writer_value, name, field)
            lb.avro_value_set_string_f(field, val)
        end
end

-- Function encode data to avro buffer
local function encode(schema, data, buffer, buf_len)
    local i = nil
    local err = ffi.new('avro_schema_error_t[?]', 1)
    local writer_schema = ffi.new('avro_schema_t[?]', 1)
    local writer_value = ffi.new('avro_value_t')

    local sch = cjson.encode(schema)
    i = lb.avro_schema_from_json(sch, 0, writer_schema, err)
    local writer_iface = lb.avro_generic_class_from_schema(writer_schema[0])
    
    i = lb.avro_generic_value_new(writer_iface, writer_value)
    
    -- Create buffer for data convert
    local writer = lb.avro_writer_memory(buffer, buf_len);
    local field = ffi.new('avro_value_t[?]', 1)
    
    local data_count = 0

    for key, dat in pairs(data) do
        local j = 0
        for k,v in pairs(schema['fields']) do
            j = j + 1
            local name = v['name']
            local val = dat[j]
            local v_type = v['type']
            print ('#write#: '..name..' = '.. val..' with type = '..v_type)
            value_set(name, val, v_type, writer_value, field)
            data_count = data_count + #tostring(val)
        end
        i = lb.avro_value_write(writer, writer_value)
    end
    return data_count, ffi.string(buffer)
end


-- Function decode avro data from buffer
local function decode(schema, buffer, count)
    local i = nil
    local err = ffi.new('avro_schema_error_t[?]', 1)
    local writer_schema = ffi.new('avro_schema_t[?]', 1)
    local writer_value = ffi.new('avro_value_t')
    
    local sch = cjson.encode(schema)
    i = lb.avro_schema_from_json(sch, 0, writer_schema, err)
    local writer_iface = lb.avro_generic_class_from_schema(writer_schema[0])
    -- Create reader buffer for avro convert
    i = lb.avro_generic_value_new(writer_iface, writer_value)
    local rd = lb.avro_reader_memory(buffer, count);
    
    local result = {}
    --print (lb.avro_value_read(rd, writer_value))
    while (lb.avro_value_read(rd, writer_value)==0) do
        local field = ffi.new('avro_value_t[?]', 1)
        local t = {}
        for k,v in pairs(schema['fields']) do
            local name = v['name']
            local v_type = v['type']
            local val = value_get(name, v_type, writer_value, field)
            t[name] = val
        end
        table.insert(result, t)
    end
    return result
end 

return {
    encode = encode,
    decode = decode
}
