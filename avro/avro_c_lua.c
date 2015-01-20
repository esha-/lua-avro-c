/*
  This module consist of function declarations that
  using in avro-lua module.
  [gh]: https://github.com/esha-/luvr
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


void hello(x) {
    printf("Hello world-avro\n");
}

void avro_value_get_by_name_f (avro_value_t *dest, const char *name, avro_value_t *field) {
    check_i(avro_value_get_by_name(dest, name, field, NULL));
}

//------------------------------------

void avro_value_set_int_f (avro_value_t *field, int32_t val) {
    check_i(avro_value_set_int(field, val));
}

void avro_value_get_int_f (avro_value_t *field, int32_t *val) {
    check_i(avro_value_get_int(field, val));
}

//------------------------------------

void avro_value_set_string_f (avro_value_t *field, const char* val) {
    check_i(avro_value_set_string(field, val));
}

void avro_value_get_string_f (const avro_value_t *field, const char** val, size_t* size) {
    check_i(avro_value_get_string(field, val, size));
}

//------------------------------------






