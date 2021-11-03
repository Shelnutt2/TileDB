/**
* @file   tiledb_file.cc
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
* This file defines the C API of TileDB for tiledb_file_t.
*/

#include "tiledb_experimental.h"

int32_t tiledb_file_alloc(tiledb_ctx_t* ctx, const char* array_uri, tiledb_file_t* file, tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *array = nullptr;
    return TILEDB_ERR;
  }

  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status::Error(
        "Failed to create TileDB array object; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st =
        Status::Error("Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  (*array)->array_ =
      new (std::nothrow) tiledb::sm::Array(uri, ctx->ctx_->storage_manager());
  if ((*array)->array_ == nullptr) {
    delete *array;
    *array = nullptr;
    auto st = Status::Error(
        "Failed to create TileDB array object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_file_open(tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type) {

}

int32_t tiledb_file_create_default(tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config) {

}

int32_t tiledb_file_create_from_path(tiledb_ctx_t* ctx, tiledb_file_t* file,const char* input_uri, tiledb_config_t* config) {

}

int32_t tiledb_file_create_from_vfs_fh(tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_vfs_fh_t* input, tiledb_config_t* config) {

}


int32_t tiledb_file_store_raw(tiledb_ctx_t* ctx, tiledb_file_t* file, void* bytes, uint64_t size) {

}

int32_t tiledb_file_store_path(tiledb_ctx_t* ctx, tiledb_file_t* file, const char*, tiledb_config_t* config) {

}

int32_t tiledb_file_store_vfs_fh(tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_vfs_fh_t*) {

}


int32_t tiledb_file_get_mime(tiledb_ctx_t* ctx, tiledb_file_t* file, const char*) {

}

int32_t tiledb_file_get_original_name(tiledb_ctx_t* ctx, tiledb_file_t* file, const char**) {

}

int32_t tiledb_file_get_extension(tiledb_ctx_t* ctx, tiledb_file_t* file, const char**) {

}

int32_t tiledb_file_get_schema( tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_array_schema_t** array_schema){

}

int32_t tiledb_file_export_raw(tiledb_ctx_t* ctx, tiledb_file_t* file, void* bytes) {

}

int32_t tiledb_file_export_path(tiledb_ctx_t* ctx, tiledb_file_t* file, char*, tiledb_config_t* config) {

}

int32_t tiledb_file_export_vfs_fh(tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_vfs_fh_t*) {

}
