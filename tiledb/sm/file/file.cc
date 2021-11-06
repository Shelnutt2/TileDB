/**
 * @file   file.cc
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
 * This file implements class File.
 */

#include "tiledb/sm/file/file.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/query/query.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

File::File(const URI& array_uri, StorageManager* storage_manager)
    : Array(array_uri, storage_manager)
    , original_file_uri_("")
    , file_schema_()
    , offset_(0) {
  // We want to default these incase the user doesn't set it.
  // This is required for writes to the query and the metadata get the same
  // timestamp
  timestamp_end_ = utils::time::timestamp_now_ms();
  timestamp_end_opened_at_ = timestamp_end_;
};

/* ********************************* */
/*                API                */
/* ********************************* */

Status File::open(
    QueryType query_type,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  //  Array::open(query_type, encryption_type, encryption_key, key_length);
  return Array::open(
      query_type,
      timestamp_start_,
      timestamp_end_,
      encryption_type,
      encryption_key,
      key_length);
}

Status File::open(
    QueryType query_type,
    uint64_t timestamp_start,
    uint64_t timestamp_end,
    EncryptionType encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  Array::open(
      query_type,
      timestamp_start,
      timestamp_end,
      encryption_type,
      encryption_key,
      key_length);

  //  if (query_type == QueryType::READ) {
  //    get_original_file_uri()
  //  }
}

void File::set_original_file_uri(const URI& original_file_uri) {
  original_file_uri_ = original_file_uri;
}

Status File::create(const Config* config) {
  try {
    auto encryption_key = get_encryption_key_from_config(config_);
    RETURN_NOT_OK(storage_manager_->array_create(
        array_uri_, &file_schema_, *encryption_key));
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::create_from_uri(const URI& file, const Config* config) {
  VFS vfs;
  // Initialize VFS object
  auto stats = storage_manager_->stats();
  auto compute_tp = storage_manager_->compute_tp();
  auto io_tp = storage_manager_->io_tp();
  auto vfs_config = config ? config : nullptr;
  auto ctx_config = storage_manager_->config();
  RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

  VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_READ);

  auto st = create_from_vfs_fh(&vfsfh, config);
  auto vfs_st = vfs.terminate();
  if(!vfs_st.ok())
    LOG_STATUS(vfs_st);
  return st;
}

Status File::create_from_vfs_fh(
    const VFSFileHandle* file, const Config* config) {
  try {
    if (file->mode() != VFSMode::VFS_READ)
      return Status::FileError("File must be open in READ mode");

    uint64_t size = file->size();
    file_schema_.set_schema_based_on_file_details(size, false);
    auto encrpytion_key = get_encryption_key_from_config(config_);
    RETURN_NOT_OK(storage_manager_->array_create(
        array_uri_, &file_schema_, *encrpytion_key));
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::save_from_file_handle(FILE* in, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status::FileError(
          "Can not save file; File opened in read mode not write mode");

    // Get file size
    fseek(in, 0L, SEEK_END);
    uint64_t size = ftell(in);
    rewind(in);

    // TODO: Add config option to let the user control how much of the file we
    // we read
    // We can support partial writes either global order (single fragment)
    // or row-major with multiple fragment but same timestamp
    Buffer buffer;
    buffer.realloc(size);
    fread(buffer.data(), 1, size, in);

    RETURN_NOT_OK(save_from_buffer(buffer.data(), size, config));

    //    std::string uri_string = file->uri().to_string();
    //    put_metadata(
    //        constants::file_metadata_original_file_name_key.c_str(),
    //        Datatype::STRING_ASCII,
    //        uri_string.size(),
    //        uri_string.c_str());
    // TODO: add these
    //    put_metadata(constants::file_metadata_ext_key.c_str(),
    //    Datatype::STRING_ASCII, uri_string.size(), uri_string.c_str());
    //    put_metadata(constants::file_metadata_mime_key.c_str(),
    //    Datatype::STRING_ASCII, uri_string.size(), uri_string.c_str());
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::save_from_uri(const URI& file, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status::FileError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    VFS vfs;
    // Initialize VFS object
    auto stats = storage_manager_->stats();
    auto compute_tp = storage_manager_->compute_tp();
    auto io_tp = storage_manager_->io_tp();
    auto vfs_config = config ? config : nullptr;
    auto ctx_config = storage_manager_->config();
    RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

    VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_READ);

    return save_from_vfs_fh(&vfsfh, config);
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::save_from_vfs_fh(VFSFileHandle* file, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status::FileError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    if (file->mode() != VFSMode::VFS_READ)
      return Status::FileError("File must be open in READ mode");

    // TODO: Add config option to let the user control how much of the file we
    // we read
    // We can support partial writes either global order (single fragment)
    // or row-major with multiple fragment but same timestamp
    uint64_t size = file->size();
    Buffer buffer;
    buffer.realloc(size);
    RETURN_NOT_OK(file->read(0, buffer.data(), size));
    RETURN_NOT_OK(save_from_buffer(buffer.data(), size, config));

    std::string uri_string = file->uri().to_string();
    put_metadata(
        constants::file_metadata_original_file_name_key.c_str(),
        Datatype::STRING_ASCII,
        uri_string.size(),
        uri_string.c_str());
    // TODO: add these
    //    put_metadata(constants::file_metadata_ext_key.c_str(),
    //    Datatype::STRING_ASCII, uri_string.size(), uri_string.c_str());
    //    put_metadata(constants::file_metadata_mime_key.c_str(),
    //    Datatype::STRING_ASCII, uri_string.size(), uri_string.c_str());
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::save_from_buffer(void* data, uint64_t size, const Config* config) {
  try {
    if (query_type_ != QueryType::WRITE)
      return Status::FileError(
          "Can not save file; File opened in read mode; Reopen in write mode");

    Query query(storage_manager_, this);

    // Set write buffer
    RETURN_NOT_OK(
        query.set_buffer(constants::file_attribute_name, data, &size));
    std::array<uint64_t, 2> subarray = {0, size - 1};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&subarray));
    RETURN_NOT_OK(query.submit());

    RETURN_NOT_OK(put_metadata(
        constants::file_metadata_size_key.c_str(), Datatype::UINT64, 1, &size));

  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }

  return Status::Ok();
}

Status File::export_to_file_handle(FILE* out, const Config* config) {
  try {
    if (query_type_ != QueryType::READ)
      return Status::FileError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    uint64_t file_size = size();
    uint64_t buffer_size = file_size;
    Buffer data;
    data.realloc(buffer_size);

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: Add config option to let the user control how much of the file we
    // we read
    // TODO: handle offset reading
    RETURN_NOT_OK(query.set_buffer(
        constants::file_attribute_name, data.data(), &buffer_size));
    std::array<uint64_t, 2> subarray = {offset_, offset_ + file_size - 1};

    do {
      // Set subarray
      RETURN_NOT_OK(query.set_subarray(&subarray));
      RETURN_NOT_OK(query.submit());

      // Check if query could not be completed
      if (buffer_size == 0)
        return Status::FileError(
            "Unable to export entire file; Query not able to complete with "
            "records");

      uint64_t written_bytes = fwrite(data.data(), 1, buffer_size, out);
      if (written_bytes != buffer_size)
        global_logger().warn(
            "File export wrote " + std::to_string(written_bytes) +
            " but file size is " + std::to_string(file_size) +
            ". The export likely is incomplete.");

    } while (query.status() != QueryStatus::COMPLETED);

  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }
  return Status::Ok();
}

Status File::export_to_uri(const URI& file, const Config* config) {
  try {
    if (query_type_ != QueryType::READ)
      return Status::FileError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");
    VFS vfs;
    // Initialize VFS object
    auto stats = storage_manager_->stats();
    auto compute_tp = storage_manager_->compute_tp();
    auto io_tp = storage_manager_->io_tp();
    auto vfs_config = config ? config : nullptr;
    auto ctx_config = storage_manager_->config();
    RETURN_NOT_OK(vfs.init(stats, compute_tp, io_tp, &ctx_config, vfs_config));

    VFSFileHandle vfsfh(file, &vfs, VFSMode::VFS_WRITE);

    return export_to_vfs_fh(&vfsfh, config);
  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }
  return Status::Ok();
}

Status File::export_to_vfs_fh(VFSFileHandle* file, const Config* config) {
  try {
    if (query_type_ != QueryType::READ)
      return Status::FileError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    if (file->mode() != VFSMode::VFS_WRITE && file->mode() != VFSMode::VFS_APPEND)
      return Status::FileError("File must be open in WRITE OR APPEND mode");

    uint64_t file_size = size();
    uint64_t buffer_size = file_size;
    Buffer data;
    data.realloc(buffer_size);

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: Add config option to let the user control how much of the file we
    // we read
    // TODO: handle offset reading
    RETURN_NOT_OK(query.set_buffer(
        constants::file_attribute_name, data.data(), &buffer_size));
    std::array<uint64_t, 2> subarray = {offset_, offset_ + file_size - 1};

    do {
      // Set subarray
      RETURN_NOT_OK(query.set_subarray(&subarray));
      RETURN_NOT_OK(query.submit());

      // Check if query could not be completed
      if (buffer_size == 0)
        return Status::FileError(
            "Unable to export entire file; Query not able to complete with "
            "records");

      file->write(data.data(), buffer_size);

    } while (query.status() != QueryStatus::COMPLETED);

  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }
  return Status::Ok();
}

Status File::export_to_buffer(
    void* data, uint64_t* size, const Config* config) {
  try {
    if (query_type_ != QueryType::READ)
      return Status::FileError(
          "Can not export file; File opened in write mode; Reopen in read "
          "mode");

    Query query(storage_manager_, this);

    // Set read buffer
    // TODO: handle offset reading
    RETURN_NOT_OK(query.set_buffer(constants::file_attribute_name, data, size));
    std::array<uint64_t, 2> subarray = {offset_, offset_ + *size - 1};

    // Set subarray
    RETURN_NOT_OK(query.set_subarray(&subarray));
    RETURN_NOT_OK(query.submit());

  } catch (const std::exception& e) {
    return Status::FileError(e.what());
  }
  return Status::Ok();
}

// std::optional<EncryptionKey> File::get_encryption_key_from_config(const
// Config& config) const {
tdb_unique_ptr<EncryptionKey> File::get_encryption_key_from_config(
    const Config& config) const {
  std::string encryption_key_from_cfg;
  const char* encryption_key_cstr = nullptr;
  EncryptionType encryption_type = EncryptionType::NO_ENCRYPTION;
  tdb_unique_ptr<EncryptionKey> encryption_key = tdb_unique_ptr<EncryptionKey>(new EncryptionKey());
  uint64_t key_length = 0;
  bool found = false;
  encryption_key_from_cfg = config.get("sm.encryption_key", &found);
  assert(found);

  if (!encryption_key_from_cfg.empty()) {
    encryption_key_cstr = encryption_key_from_cfg.c_str();
    std::string encryption_type_from_cfg;
    bool found = false;
    encryption_type_from_cfg = config_.get("sm.encryption_type", &found);
    assert(found);
    auto [st, et] = encryption_type_enum(encryption_type_from_cfg);
    THROW_NOT_OK(st);
    encryption_type = et.value();

    if (EncryptionKey::is_valid_key_length(
            encryption_type,
            static_cast<uint32_t>(encryption_key_from_cfg.size()))) {
      const UnitTestConfig& unit_test_cfg = UnitTestConfig::instance();
      if (unit_test_cfg.array_encryption_key_length.is_set()) {
        key_length = unit_test_cfg.array_encryption_key_length.get();
      } else {
        key_length = static_cast<uint32_t>(encryption_key_from_cfg.size());
      }
    } else {
      encryption_key_cstr = nullptr;
      key_length = 0;
    }
  }

  // Copy the key bytes.
  THROW_NOT_OK(
      encryption_key->set_key(encryption_type, encryption_key_cstr, key_length));

  return encryption_key;
}

uint64_t File::size() {
  const uint64_t* size = nullptr;
  Datatype datatype = Datatype::UINT64;
  uint32_t val_num = 1;
  THROW_NOT_OK(get_metadata(
      constants::file_metadata_size_key.c_str(),
      &datatype,
      &val_num,
      reinterpret_cast<const void**>(&size)));

  if (size == nullptr)
    return 0;

  return *size;
}

// void File::get_magic();

}  // namespace sm
}  // namespace tiledb
