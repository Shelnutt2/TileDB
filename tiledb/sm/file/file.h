/**
 * @file   file.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 This file defines class Array.
 */

#ifndef TILEDB_FILE_H
#define TILEDB_FILE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/file/file_schema.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
/**
 * An file object to be opened for reads/writes. An ``File`` instance
 * is associated with the timestamp it is opened at.
 */
class File : public Array {
 public:
  File(const URI& array_uri, StorageManager* storage_manager);

  Status open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length) override;

  Status open(
      QueryType query_type,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length) override;

  void set_original_file_uri(const URI& original_file_uri);

  Status create(const Config* config);

  Status create_from_uri(const URI& file, const Config* config);

  Status create_from_vfs_fh(const VFSFileHandle* file, const Config* config);

  Status save_from_file_handle(FILE* in, const Config* config);

  Status save_from_uri(const URI& file, const Config* config);

  Status save_from_vfs_fh(VFSFileHandle* file, const Config* config);

  Status save_from_buffer(void* data, uint64_t size, const Config* config);

  Status export_to_file_handle(FILE* out, const Config* config);

  Status export_to_uri(const URI& file, const Config* config);

  Status export_to_vfs_fh(VFSFileHandle* file, const Config* config);

  Status export_to_buffer(void* data, uint64_t* size, const Config* config);

  uint64_t size();

  //  Status load_original_file_uri();
  //  Status load__uri();
  //  Status load_original_file_uri();
  //  Status load_original_file_uri();

 private:
  //  std::optional<EncryptionKey> get_encryption_key_from_config(const Config&
  //  config) const;
  const EncryptionKey& get_encryption_key_from_config(
      const Config& config) const;

  URI original_file_uri_;

  FileSchema file_schema_;

  uint64_t offset_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILE_H
