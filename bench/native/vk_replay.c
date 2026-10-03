/*
 * Native SQLite replay control for bench/sqlite_exqlite_overhead_benchmark.exs.
 *
 * Built by the benchmark against the same sqlite3.c amalgamation ExQLite
 * vendors (deps/exqlite/c_src) with ExQLite's SQLite compile definitions, so
 * the SQLite engine is identical and only the calling layer differs.
 *
 * The program runs as an Erlang port ({packet, 4}). Every request is one
 * packet; every request gets exactly one reply packet.
 *
 *   'O' u8 mode, u32 len, bytes   open: mode 0 = in-memory copy of a serialized
 *                                 database image, mode 1 = database file path
 *   'E' u32 len, sql              sqlite3_exec (setup only, not timed)
 *   'S' u32 len, sql              first column of every row, '\n'-joined
 *   'P' u32 id, u32 len, sql      prepare and keep statement `id`
 *   'R' u32 count, ops...         run one sample of operations, timed
 *   'C'                           close the database
 *
 * A run operation is either
 *   u8 0, u8 count_rows, u32 statement id, u16 parameter count, parameters...
 *   u8 1, u32 len, sql            (sqlite3_exec, as Connection.exec/2 does)
 * and a parameter is
 *   u8 0 null | u8 1 i64 | u8 2 f64 bits | u8 3 u32 len text | u8 4 u32 len blob
 * with all integers big-endian.
 *
 * Replies: 'k' ok, 'e' + message on error, 't' + text for 'S', and for 'R':
 *   'r' u64 elapsed_ns, u64 rows, u64 vm_steps, u64 statements
 *
 * Rows are consumed for every statement (ExQLite materialises every stepped
 * row), but only statements flagged count_rows add to the reported row count:
 * those are the ones whose rows the product collects, which lets the harness
 * check the replay returned exactly what the captured run returned.
 *
 * Only the execution loop of 'R' is timed: the request is fully decoded first.
 * Each statement binds every parameter with SQLITE_TRANSIENT (as ExQLite
 * does), steps to SQLITE_DONE, copies every column value out of SQLite (as
 * ExQLite materialises rows), and is reset for reuse.
 */
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "sqlite3.h"

typedef struct {
    uint8_t type;
    int64_t integer;
    double real;
    const unsigned char* bytes;
    uint32_t length;
} param_t;

typedef struct {
    uint8_t kind;
    uint8_t count_rows;
    uint32_t id;
    uint16_t count;
    param_t* params;
    char* sql;
} op_t;

static sqlite3* db = NULL;
static sqlite3_stmt** statements = NULL;
static uint32_t statement_capacity = 0;
static unsigned char* scratch = NULL;
static size_t scratch_capacity = 0;
static volatile uint64_t sink = 0;

static int
read_exact(unsigned char* buffer, size_t length)
{
    size_t done = 0;
    while (done < length) {
        ssize_t got = read(STDIN_FILENO, buffer + done, length - done);
        if (got == 0) {
            return 0;
        }
        if (got < 0) {
            if (errno == EINTR) {
                continue;
            }
            return 0;
        }
        done += (size_t)got;
    }
    return 1;
}

static void
write_exact(const unsigned char* buffer, size_t length)
{
    size_t done = 0;
    while (done < length) {
        ssize_t wrote = write(STDOUT_FILENO, buffer + done, length - done);
        if (wrote < 0) {
            if (errno == EINTR) {
                continue;
            }
            exit(2);
        }
        done += (size_t)wrote;
    }
}

static void
reply(char tag, const void* payload, size_t length)
{
    unsigned char header[5];
    uint32_t total = (uint32_t)(length + 1);
    header[0] = (unsigned char)(total >> 24);
    header[1] = (unsigned char)(total >> 16);
    header[2] = (unsigned char)(total >> 8);
    header[3] = (unsigned char)total;
    header[4] = (unsigned char)tag;
    write_exact(header, 5);
    if (length > 0) {
        write_exact(payload, length);
    }
}

static void
reply_error(const char* message)
{
    reply('e', message, strlen(message));
}

static void
reply_sqlite_error(const char* context)
{
    char message[1024];
    snprintf(message, sizeof(message), "%s: %s", context, db ? sqlite3_errmsg(db) : "no database");
    reply_error(message);
}

typedef struct {
    const unsigned char* data;
    size_t length;
    size_t offset;
    int failed;
} cursor_t;

static uint8_t
take_u8(cursor_t* cursor)
{
    if (cursor->offset + 1 > cursor->length) {
        cursor->failed = 1;
        return 0;
    }
    return cursor->data[cursor->offset++];
}

static uint64_t
take_be(cursor_t* cursor, int bytes)
{
    uint64_t value = 0;
    if (cursor->offset + (size_t)bytes > cursor->length) {
        cursor->failed = 1;
        return 0;
    }
    for (int i = 0; i < bytes; i++) {
        value = (value << 8) | cursor->data[cursor->offset++];
    }
    return value;
}

static const unsigned char*
take_bytes(cursor_t* cursor, uint32_t length)
{
    const unsigned char* start;
    if (cursor->offset + length > cursor->length) {
        cursor->failed = 1;
        return NULL;
    }
    start = cursor->data + cursor->offset;
    cursor->offset += length;
    return start;
}

static char*
take_string(cursor_t* cursor)
{
    uint32_t length = (uint32_t)take_be(cursor, 4);
    const unsigned char* bytes = take_bytes(cursor, length);
    char* copy;
    if (cursor->failed) {
        return NULL;
    }
    copy = malloc(length + 1);
    memcpy(copy, bytes, length);
    copy[length] = '\0';
    return copy;
}

static void
close_database(void)
{
    for (uint32_t i = 0; i < statement_capacity; i++) {
        if (statements[i]) {
            sqlite3_finalize(statements[i]);
            statements[i] = NULL;
        }
    }
    if (db) {
        sqlite3_close(db);
        db = NULL;
    }
}

static void
handle_open(cursor_t* cursor)
{
    uint8_t mode = take_u8(cursor);
    uint32_t length = (uint32_t)take_be(cursor, 4);
    const unsigned char* bytes = take_bytes(cursor, length);
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

    if (cursor->failed) {
        reply_error("malformed open request");
        return;
    }

    close_database();

    if (mode == 1) {
        char* path = malloc(length + 1);
        memcpy(path, bytes, length);
        path[length] = '\0';
        int rc = sqlite3_open_v2(path, &db, flags, NULL);
        free(path);
        if (rc != SQLITE_OK) {
            reply_sqlite_error("open");
            return;
        }
        reply('k', NULL, 0);
        return;
    }

    /* Restore the image into a regular ":memory:" database through the backup
     * API, so the pager is the same one an ExQLite ":memory:" handle uses. */
    sqlite3* image = NULL;
    unsigned char* copy = sqlite3_malloc64(length);
    if (copy == NULL) {
        reply_error("out of memory");
        return;
    }
    memcpy(copy, bytes, length);

    if (sqlite3_open_v2(":memory:", &db, flags, NULL) != SQLITE_OK ||
        sqlite3_open_v2(":memory:", &image, flags, NULL) != SQLITE_OK) {
        sqlite3_free(copy);
        reply_sqlite_error("open memory");
        return;
    }

    if (sqlite3_deserialize(image, "main", copy, length, length,
                            SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE) !=
        SQLITE_OK) {
        sqlite3_close(image);
        reply_error("deserialize failed");
        return;
    }

    sqlite3_backup* backup = sqlite3_backup_init(db, "main", image, "main");
    if (backup == NULL) {
        sqlite3_close(image);
        reply_sqlite_error("backup init");
        return;
    }
    int rc = sqlite3_backup_step(backup, -1);
    sqlite3_backup_finish(backup);
    sqlite3_close(image);

    if (rc != SQLITE_DONE) {
        reply_sqlite_error("backup");
        return;
    }
    reply('k', NULL, 0);
}

static void
handle_exec(cursor_t* cursor)
{
    char* sql = take_string(cursor);
    char* error = NULL;
    if (sql == NULL) {
        reply_error("malformed exec request");
        return;
    }
    if (sqlite3_exec(db, sql, NULL, NULL, &error) != SQLITE_OK) {
        reply_error(error ? error : "exec failed");
        sqlite3_free(error);
    } else {
        reply('k', NULL, 0);
    }
    free(sql);
}

static void
handle_scalar(cursor_t* cursor)
{
    char* sql = take_string(cursor);
    sqlite3_stmt* statement = NULL;
    size_t used = 0;
    size_t capacity = 256;
    char* text = malloc(capacity);

    if (sql == NULL) {
        free(text);
        reply_error("malformed query request");
        return;
    }

    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        free(sql);
        free(text);
        reply_sqlite_error("prepare query");
        return;
    }

    while (sqlite3_step(statement) == SQLITE_ROW) {
        const unsigned char* value = sqlite3_column_text(statement, 0);
        size_t length = (size_t)sqlite3_column_bytes(statement, 0);
        if (used + length + 1 > capacity) {
            capacity = (used + length + 1) * 2;
            text = realloc(text, capacity);
        }
        if (used > 0) {
            text[used++] = '\n';
        }
        if (value) {
            memcpy(text + used, value, length);
            used += length;
        }
    }

    sqlite3_finalize(statement);
    reply('t', text, used);
    free(text);
    free(sql);
}

static void
handle_prepare(cursor_t* cursor)
{
    uint32_t id = (uint32_t)take_be(cursor, 4);
    char* sql = take_string(cursor);
    if (sql == NULL) {
        reply_error("malformed prepare request");
        return;
    }

    if (id >= statement_capacity) {
        uint32_t capacity = statement_capacity ? statement_capacity : 16;
        while (capacity <= id) {
            capacity *= 2;
        }
        statements = realloc(statements, capacity * sizeof(sqlite3_stmt*));
        memset(statements + statement_capacity, 0,
               (capacity - statement_capacity) * sizeof(sqlite3_stmt*));
        statement_capacity = capacity;
    }

    if (statements[id]) {
        sqlite3_finalize(statements[id]);
        statements[id] = NULL;
    }

    if (sqlite3_prepare_v3(db, sql, -1, 0, &statements[id], NULL) !=
        SQLITE_OK) {
        free(sql);
        reply_sqlite_error("prepare");
        return;
    }
    free(sql);
    reply('k', NULL, 0);
}

static void
ensure_scratch(size_t length)
{
    if (length > scratch_capacity) {
        scratch_capacity = length * 2;
        scratch = realloc(scratch, scratch_capacity);
    }
}

static void
consume_row(sqlite3_stmt* statement)
{
    int columns = sqlite3_column_count(statement);
    for (int i = 0; i < columns; i++) {
        switch (sqlite3_column_type(statement, i)) {
            case SQLITE_INTEGER:
                sink += (uint64_t)sqlite3_column_int64(statement, i);
                break;
            case SQLITE_FLOAT:
                sink += (uint64_t)(int64_t)sqlite3_column_double(statement, i);
                break;
            case SQLITE_TEXT: {
                const unsigned char* text = sqlite3_column_text(statement, i);
                size_t length = (size_t)sqlite3_column_bytes(statement, i);
                ensure_scratch(length);
                memcpy(scratch, text, length);
                sink += length ? scratch[length - 1] : 0;
                break;
            }
            case SQLITE_BLOB: {
                const void* blob = sqlite3_column_blob(statement, i);
                size_t length = (size_t)sqlite3_column_bytes(statement, i);
                ensure_scratch(length);
                if (length) {
                    memcpy(scratch, blob, length);
                    sink += scratch[length - 1];
                }
                break;
            }
            default:
                break;
        }
    }
}

static int
bind_params(sqlite3_stmt* statement, op_t* op)
{
    for (uint16_t i = 0; i < op->count; i++) {
        param_t* param = &op->params[i];
        int index = i + 1;
        int rc;
        switch (param->type) {
            case 0:
                rc = sqlite3_bind_null(statement, index);
                break;
            case 1:
                rc = sqlite3_bind_int64(statement, index, param->integer);
                break;
            case 2:
                rc = sqlite3_bind_double(statement, index, param->real);
                break;
            case 3:
                rc = sqlite3_bind_text(statement, index, (const char*)param->bytes,
                                       (int)param->length, SQLITE_TRANSIENT);
                break;
            default:
                rc = sqlite3_bind_blob(statement, index, param->bytes, (int)param->length,
                                       SQLITE_TRANSIENT);
                break;
        }
        if (rc != SQLITE_OK) {
            return rc;
        }
    }
    return SQLITE_OK;
}

static int
decode_ops(cursor_t* cursor, op_t* ops, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        op_t* op = &ops[i];
        op->kind = take_u8(cursor);
        if (op->kind == 1) {
            op->sql = take_string(cursor);
            if (op->sql == NULL) {
                return 0;
            }
            continue;
        }
        op->count_rows = take_u8(cursor);
        op->id = (uint32_t)take_be(cursor, 4);
        op->count = (uint16_t)take_be(cursor, 2);
        if (cursor->failed || op->id >= statement_capacity || statements[op->id] == NULL) {
            return 0;
        }
        op->params = calloc(op->count ? op->count : 1, sizeof(param_t));
        for (uint16_t p = 0; p < op->count; p++) {
            param_t* param = &op->params[p];
            param->type = take_u8(cursor);
            switch (param->type) {
                case 0:
                    break;
                case 1:
                    param->integer = (int64_t)take_be(cursor, 8);
                    break;
                case 2: {
                    uint64_t bits = take_be(cursor, 8);
                    memcpy(&param->real, &bits, sizeof(double));
                    break;
                }
                case 3:
                case 4:
                    param->length = (uint32_t)take_be(cursor, 4);
                    param->bytes = take_bytes(cursor, param->length);
                    break;
                default:
                    cursor->failed = 1;
            }
        }
        if (cursor->failed) {
            return 0;
        }
    }
    return !cursor->failed;
}

static void
free_ops(op_t* ops, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        free(ops[i].params);
        free(ops[i].sql);
    }
    free(ops);
}

static uint64_t
now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void
put_u64(unsigned char* out, uint64_t value)
{
    for (int i = 7; i >= 0; i--) {
        out[i] = (unsigned char)value;
        value >>= 8;
    }
}

static void
handle_run(cursor_t* cursor)
{
    uint32_t count = (uint32_t)take_be(cursor, 4);
    op_t* ops = calloc(count ? count : 1, sizeof(op_t));
    uint64_t rows = 0;
    uint64_t executed = 0;
    uint64_t vm_steps = 0;
    char* error = NULL;
    const char* failure = NULL;

    if (cursor->failed || !decode_ops(cursor, ops, count)) {
        free_ops(ops, count);
        reply_error("malformed run request");
        return;
    }

    uint64_t started = now_ns();

    for (uint32_t i = 0; i < count && failure == NULL; i++) {
        op_t* op = &ops[i];
        if (op->kind == 1) {
            if (sqlite3_exec(db, op->sql, NULL, NULL, &error) != SQLITE_OK) {
                failure = "exec";
            }
            executed++;
            continue;
        }

        sqlite3_stmt* statement = statements[op->id];
        if (bind_params(statement, op) != SQLITE_OK) {
            failure = "bind";
            break;
        }
        for (;;) {
            int rc = sqlite3_step(statement);
            if (rc == SQLITE_ROW) {
                consume_row(statement);
                rows += op->count_rows;
            } else if (rc == SQLITE_DONE) {
                break;
            } else {
                failure = "step";
                break;
            }
        }
        sqlite3_reset(statement);
        executed++;
    }

    uint64_t elapsed = now_ns() - started;

    for (uint32_t i = 0; i < statement_capacity; i++) {
        if (statements[i]) {
            vm_steps += (uint64_t)sqlite3_stmt_status(statements[i], SQLITE_STMTSTATUS_VM_STEP, 1);
        }
    }

    free_ops(ops, count);

    if (failure) {
        char message[1024];
        snprintf(message, sizeof(message), "%s failed: %s", failure,
                 error ? error : sqlite3_errmsg(db));
        sqlite3_free(error);
        reply_error(message);
        return;
    }

    unsigned char out[32];
    put_u64(out, elapsed);
    put_u64(out + 8, rows);
    put_u64(out + 16, vm_steps);
    put_u64(out + 24, executed);
    reply('r', out, sizeof(out));
}

int
main(void)
{
    unsigned char header[4];
    unsigned char* buffer = NULL;
    size_t capacity = 0;

    while (read_exact(header, 4)) {
        size_t length = ((size_t)header[0] << 24) | ((size_t)header[1] << 16) |
                        ((size_t)header[2] << 8) | (size_t)header[3];
        if (length == 0) {
            continue;
        }
        if (length > capacity) {
            capacity = length;
            buffer = realloc(buffer, capacity);
        }
        if (!read_exact(buffer, length)) {
            break;
        }

        cursor_t cursor = {buffer + 1, length - 1, 0, 0};
        char tag = (char)buffer[0];

        if (tag != 'O' && tag != 'C' && db == NULL) {
            reply_error("database is not open");
            continue;
        }

        switch (tag) {
            case 'O':
                handle_open(&cursor);
                break;
            case 'E':
                handle_exec(&cursor);
                break;
            case 'S':
                handle_scalar(&cursor);
                break;
            case 'P':
                handle_prepare(&cursor);
                break;
            case 'R':
                handle_run(&cursor);
                break;
            case 'C':
                close_database();
                reply('k', NULL, 0);
                break;
            default:
                reply_error("unknown request");
        }
    }

    close_database();
    free(buffer);
    free(scratch);
    return 0;
}
