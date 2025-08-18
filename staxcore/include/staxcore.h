#ifndef STAXCORE_H
#define STAXCORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointers for the database and cursor handles.
typedef struct stax_db_t stax_db_t;
typedef struct stax_cursor_t stax_cursor_t;

// --- Status Codes ---

// Defines all possible status codes returned by the API.
typedef enum {
    STAX_OK = 0,
    STAX_ERROR_NOT_FOUND = 1,
    STAX_ERROR_GENERIC = -1,
    STAX_ERROR_INVALID_ARG = -2,
    STAX_ERROR_OUT_OF_MEMORY = -3,
    STAX_ERROR_READ_ONLY = -4,
    STAX_ERROR_MMAP_FAILED = -5,
    STAX_ERROR_FILE_OPEN_FAILED = -6,
    STAX_ERROR_FILE_TRUNCATE_FAILED = -7,
    STAX_ERROR_FILE_RENAME_FAILED = -8,
    STAX_ERROR_DB_OPEN_FAILED = -9,
    STAX_ERROR_COMPACTION_FAILED = -10,
    STAX_ERROR_FLATTEN_FAILED = -11,
    STAX_ERROR_NOT_APPLICABLE = -12,
} stax_status_t;

// Returns a human-readable string for a given status code.
const char* stax_strerror(stax_status_t status);


// --- Configuration ---

// Defines the storage backend type.
typedef enum {
    STAX_STORE_IN_MEMORY,
    STAX_STORE_ANONYMOUS,
    STAX_STORE_FILE
} stax_storage_type_t;

// Configuration options for opening a database.
typedef struct {
    stax_storage_type_t storage_type;
    const char* filepath;
    uint64_t db_size;
    size_t thread_arena_size;
    bool read_only;
} stax_config_t;

stax_config_t stax_get_default_config();


// --- Database Operations ---

stax_status_t stax_db_open(const stax_config_t* config, stax_db_t** db);
void stax_db_close(stax_db_t* db);

// --- Key/Value Operations ---

typedef struct {
    const void* data;
    size_t size;
} stax_slice_t;

stax_status_t stax_put(stax_db_t* db, const stax_slice_t* key, const stax_slice_t* value);
stax_status_t stax_get(stax_db_t* db, const stax_slice_t* key, stax_slice_t* value);
stax_status_t stax_delete(stax_db_t* db, const stax_slice_t* key);
void stax_free_slice(stax_slice_t* value);


// --- Advanced Operations ---

stax_status_t stax_compact(stax_db_t* db);
stax_status_t stax_flatten(stax_db_t* db);


// --- Cursor Operations ---

stax_status_t stax_cursor_create(stax_db_t* db, stax_cursor_t** cursor);
void stax_cursor_destroy(stax_cursor_t* cursor);
bool stax_cursor_valid(stax_cursor_t* cursor);
void stax_cursor_seek_first(stax_cursor_t* cursor);
void stax_cursor_seek(stax_cursor_t* cursor, const stax_slice_t* key);
void stax_cursor_next(stax_cursor_t* cursor);
void stax_cursor_get(stax_cursor_t* cursor, stax_slice_t* key, stax_slice_t* value);


#ifdef __cplusplus
}
#endif

#endif // STAXCORE_H
