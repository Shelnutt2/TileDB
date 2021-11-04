/**
 * @file   tiledb_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares experimental C API for TileDB.
 * Experimental APIs to do not fall under the API compatibility guarantees and
 * might change between TileDB versions
 */

#ifndef TILEDB_EXPERIMENTAL_H
#define TILEDB_EXPERIMENTAL_H

#include "tiledb.h"

/* ********************************* */
/*               MACROS              */
/* ********************************* */

#ifdef __cplusplus
extern "C" {
#endif

/** A TileDB array schema. */
typedef struct tiledb_array_schema_evolution_t tiledb_array_schema_evolution_t;

/** TileDB file type. */
typedef struct tiledb_file_t tiledb_file_t;

/* ********************************* */
/*      ARRAY SCHEMA EVOLUTION       */
/* ********************************* */

/**
 * Creates a TileDB schema evolution object.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_t* array_schema_evolution;
 * tiledb_array_schema_evolution_alloc(ctx, &array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The TileDB schema evolution to be created.
 * @return `TILEDB_OK` for success and `TILEDB_OOM` or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Destroys an array schema evolution, freeing associated memory.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_array_schema_evolution_free(&array_schema_evolution);
 * @endcode
 *
 * @param array_schema_evolution The array schema evolution to be destroyed.
 */
TILEDB_EXPORT void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution);

/**
 * Adds an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_attribute_t* attr;
 * tiledb_attribute_alloc(ctx, "my_attr", TILEDB_INT32, &attr);
 * tiledb_array_schema_evolution_add_attribute(ctx, array_schema_evolution,
 * attr);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attr The attribute to be added.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attribute);

/**
 * Drops an attribute to an array schema evolution.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* attribute_name="a1";
 * tiledb_array_schema_evolution_drop_attribute(ctx, array_schema_evolution,
 * attribute_name);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_schema_evolution The schema evolution.
 * @param attribute_name The name of the attribute to be dropped.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name);

/* ********************************* */
/*               ARRAY               */
/* ********************************* */

/**
 * Evolve array schema of an array.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_evolve(ctx, array_uri,array_schema_evolution);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param array_schema_evolution The schema evolution.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution);

/**
 * Upgrades an array to the latest format version.
 *
 * **Example:**
 *
 * @code{.c}
 * const char* array_uri="test_array";
 * tiledb_array_upgrade_version(ctx, array_uri);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param array_uri The uri of the array.
 * @param config Configuration parameters for the upgrade
 *     (`nullptr` means default, which will use the config from `ctx`).
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config);

/* ********************************* */
/*              FILE                 */
/* ********************************* */

TILEDB_EXPORT int32_t tiledb_file_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_file_t** file,
    tiledb_config_t* config);

TILEDB_EXPORT int32_t tiledb_file_create_default(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_create_from_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_create_from_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config);

TILEDB_EXPORT int32_t tiledb_file_store_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* in, tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_store_raw(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t size,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_store_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char*,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_store_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t*,
    tiledb_config_t* config);

TILEDB_EXPORT int32_t
tiledb_file_get_mime(tiledb_ctx_t* ctx, tiledb_file_t* file, const char*);
TILEDB_EXPORT int32_t tiledb_file_get_original_name(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char**);
TILEDB_EXPORT int32_t
tiledb_file_get_extension(tiledb_ctx_t* ctx, tiledb_file_t* file, const char**);
TILEDB_EXPORT int32_t tiledb_file_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_array_schema_t** array_schema);

TILEDB_EXPORT int32_t tiledb_file_export_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* out, tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_export_raw(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t* size,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_export_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* output_uri,
    tiledb_config_t* config);
TILEDB_EXPORT int32_t tiledb_file_export_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* output,
    tiledb_config_t* config);

/**
 * Sets the starting timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The default value is `0`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the timestamp on.
 * @param timestamp_start The epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_start);

/**
 * Sets the ending timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The UINT64_MAX timestamp is a reserved timestamp
 * that will be interpretted as the current timestamp when an file is opened.
 * The default value is `UINT64_MAX`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the timestamp on.
 * @param timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX for
 *   the current timestamp.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_end);

/**
 * Gets the starting timestamp used when opening (and reopening) the file.
 * This is an inclusive bound.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_start;
 * tiledb_file_get_open_timestamp_start(ctx, file, &timestamp_start);
 * assert(timestamp_start == 1234);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the timestamp on.
 * @param timestamp_start The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_start);

/**
 * Gets the ending timestamp used when opening (and reopening) the file.
 * This is an inclusive bound. If UINT64_MAX was set, this will return
 * the timestamp at the time the file was opened. If the file has not
 * yet been opened, it will return UINT64_MAX.`
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_end;
 * tiledb_file_get_open_timestamp_end(ctx, file, &timestamp_end);
 * assert(timestamp_start == 5678);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file to set the timestamp on.
 * @param timestamp_end The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_end);

/**
 * Opens a TileDB file. The file is opened using a query type as input.
 * This is to indicate that queries created for this `tiledb_file_t`
 * object will inherit the query type. In other words, `tiledb_file_t`
 * objects are opened to receive only one type of queries.
 * They can always be closed and be re-opened with another query type.
 * Also there may be many different `tiledb_file_t`
 * objects created and opened with different query types.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object to be opened.
 * @param query_type The type of queries the file object will be receiving.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the same file object is opened again without being closed,
 *     an error will be thrown.
 * @note The config should be set before opening an file.
 * @note If the file is to be opened at a specfic time interval, the
 *      `timestamp{start, end}` values should be set to a config that's set to
 *       the file object before opening the file.
 */
TILEDB_EXPORT int32_t tiledb_file_open(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type);

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_EXPERIMENTAL_H
