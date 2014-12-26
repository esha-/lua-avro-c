/*
	This example shows how to write data into an Avro container buffer, and how to	
	read data from a buffer.
	[gh]: https://github.com/esha-/luvr
	original (avro read-write from file)
	[gh]: https://github.com/dcreager/avro-examples/tree/master/resolved-writer
*/

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <avro.h>

// These macros help us check for Avro errors.  If any occur, we print out an
// error message and abort the process.

#define check_i(call) \
    do { \
        if ((call) != 0) { \
            fprintf(stderr, "Error: %s\n", avro_strerror()); \
            exit(EXIT_FAILURE); \
        } \
    } while (0)

#define check_p(call) \
    do { \
        if ((call) == NULL) { \
            fprintf(stderr, "Error: %s\n", avro_strerror()); \
            exit(EXIT_FAILURE); \
        } \
    } while (0)

// Schemas

#define WRITER_SCHEMA \
    "{" \
    "  \"type\": \"record\"," \
    "  \"name\": \"test\"," \
    "  \"fields\": [" \
    "    { \"name\": \"a\", \"type\": \"int\" }," \
    "    { \"name\": \"b\", \"type\": \"int\" }" \
    "  ]" \
    "}"
// Buffers
char write_buf[4096];
char read_buf[4096];

// This function writes a sequence of integers into a new Avro data file, using
// the `WRITER_SCHEMA`.

static void write_data(const char * schema) {
    avro_writer_t wrt;
    avro_schema_t  writer_schema;
    avro_schema_error_t  error;
    avro_value_iface_t  *writer_iface;
    avro_value_t  writer_value;
    avro_value_t  field;

    // First parse the JSON schema into the C API's internal schema
    // representation.
    check_i(avro_schema_from_json(schema, 0, &writer_schema, &error));

    // Create a value that is an instance of that schema.
    check_p(writer_iface = avro_generic_class_from_schema(writer_schema));
    check_i(avro_generic_value_new(writer_iface, &writer_value));

    // Create memory buffer for writing
    wrt = avro_writer_memory(write_buf, 2);
    
    // Data record
    check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
    check_i(avro_value_set_int(&field, 10));
    check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
    check_i(avro_value_set_int(&field, 11));
    
    // Write data in buffer
    check_i(avro_value_write(wrt, &writer_value));

    // Close the file and clean up after ourselves.
    avro_writer_free(wrt);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);
}

static void read_data(const char * schema) {
    avro_reader_t rd;
    avro_schema_t  writer_schema;
    avro_schema_error_t  error;
    avro_value_iface_t  *writer_iface;
    avro_value_t  writer_value;

    // First parse the JSON schema into the C API's internal schema
    // representation.
    check_i(avro_schema_from_json(schema, 0, &writer_schema, &error));

    // Create a value that is an instance of that schema.
    check_p(writer_iface = avro_generic_class_from_schema(writer_schema));
    check_i(avro_generic_value_new(writer_iface, &writer_value));

    // Create memory buffer for reading
    rd = avro_reader_memory(write_buf, 2);

    // Read values from the buffer until we run out
    while (avro_value_read(rd, &writer_value) == 0) {
        avro_value_t  field;
        int32_t  a;
        int32_t  b;

        check_i(avro_value_get_by_name(&writer_value, "a", &field, NULL));
        check_i(avro_value_get_int(&field, &a));
        check_i(avro_value_get_by_name(&writer_value, "b", &field, NULL));
        check_i(avro_value_get_int(&field, &b));
        printf("  a: %" PRId32 ", b: %" PRId32 "\n", a, b);
    }

     // Close the file and clean up after ourselves.
    avro_reader_free(rd);
    avro_value_decref(&writer_value);
    avro_value_iface_decref(writer_iface);
    avro_schema_decref(writer_schema);   
}

int main() {
    printf("Writing data...\n");
    write_data(WRITER_SCHEMA);

    printf("Reading data...\n");
    read_data(WRITER_SCHEMA);
    return 0;
}




















