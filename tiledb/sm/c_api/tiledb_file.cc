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

#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_helpers.h"
#include "tiledb/sm/file/file.h"

int32_t tiledb_file_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_file_t** file,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *file = nullptr;
    return TILEDB_ERR;
  }

  // Create file struct
  *file = new (std::nothrow) tiledb_file_t;
  if (*file == nullptr) {
    auto st = Status::Error(
        "Failed to create TileDB file object; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check file URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = Status::Error("Failed to create TileDB file object; Invalid URI");
    delete *file;
    *file = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an file object
  (*file)->file_ =
      new (std::nothrow) tiledb::sm::File(uri, ctx->ctx_->storage_manager());
  if ((*file)->file_ == nullptr) {
    delete *file;
    *file = nullptr;
    auto st = Status::Error(
        "Failed to create TileDB file object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_file_create_default(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create(
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_create_from_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  tiledb::sm::URI uri(input_uri);
  if (uri.is_invalid()) {
    auto st = Status::Error(
        "Failed to create file from path; Invalid input file URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create_from_uri(
              uri,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_create_from_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, input) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create_from_vfs_fh(
              input->vfs_fh_,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_store_raw(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t size,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_buffer(
              bytes, size, config ? config->config_ : nullptr))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_store_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  tiledb::sm::URI uri(input_uri);
  if (uri.is_invalid()) {
    auto st = Status::Error(
        "Failed to create file from path; Invalid input file URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_uri(
              uri, config ? config->config_ : nullptr))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_store_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, input) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_vfs_fh(
              input->vfs_fh_, config ? config->config_ : nullptr))) {
    return TILEDB_ERR;
  }
}

int32_t tiledb_file_get_mime(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char*) {
}

int32_t tiledb_file_get_original_name(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char**) {
}

int32_t tiledb_file_get_extension(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char**) {
}

int32_t tiledb_file_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_array_schema_t** array_schema) {
}

int32_t tiledb_file_export_raw(
    tiledb_ctx_t* ctx, tiledb_file_t* file, void* bytes) {
}

int32_t tiledb_file_export_uri(
    tiledb_ctx_t* ctx, tiledb_file_t* file, char*, tiledb_config_t* config) {
}

int32_t tiledb_file_export_vfs_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_vfs_fh_t*) {
}

int32_t tiledb_file_set_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_start) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, file->file_->set_timestamp_start(timestamp_start)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_set_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_end) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, file->file_->set_timestamp_end(timestamp_end)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_get_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_start) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_start = file->file_->timestamp_start();

  return TILEDB_OK;
}

int32_t tiledb_file_get_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_end) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_end = file->file_->timestamp_end_opened_at();

  return TILEDB_OK;
}

int32_t tiledb_file_open(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open file
  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0)))
    return TILEDB_ERR;

  return TILEDB_OK;
}