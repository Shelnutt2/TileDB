/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
 * This file implements class Query.
 */

#include "tiledb/sm/query/query.h"
#include "tiledb/rest/capnp/array.h"
#include "tiledb/rest/capnp/utils.h"
#include "tiledb/rest/curl/client.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

#include <cassert>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(StorageManager* storage_manager, Array* array, URI fragment_uri)
    : array_(array)
    , storage_manager_(storage_manager) {
  assert(array != nullptr && array->is_open());

  callback_ = nullptr;
  callback_data_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  status_ = QueryStatus::UNINITIALIZED;
  auto st = array->get_query_type(&type_);
  assert(st.ok());

  if (type_ == QueryType::WRITE)
    writer_.set_storage_manager(storage_manager);
  else
    reader_.set_storage_manager(storage_manager);

  if (type_ == QueryType::READ) {
    reader_.set_storage_manager(storage_manager);
    reader_.set_array(array);
    reader_.set_array_schema(array->array_schema());
    reader_.set_fragment_metadata(array->fragment_metadata());
  } else {
    writer_.set_storage_manager(storage_manager);
    writer_.set_array(array);
    writer_.set_array_schema(array->array_schema());
    writer_.set_fragment_uri(fragment_uri);
  }
}

Query::~Query() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

const Array* Query::array() const {
  return array_;
}

const ArraySchema* Query::array_schema() const {
  if (type_ == QueryType::WRITE)
    return writer_.array_schema();
  return reader_.array_schema();
}

std::vector<std::string> Query::attributes() const {
  if (type_ == QueryType::WRITE)
    return writer_.attributes();
  return reader_.attributes();
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return Status::Ok();

  if (array_->is_remote()) {
    array_->array_schema()->set_array_uri(array_->array_uri());
    Config config = this->storage_manager_->config();
    return tiledb::rest::finalize_query_to_rest(
        config, array_->array_uri().to_string(), this);
  }

  RETURN_NOT_OK(writer_.finalize());
  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

Status Query::get_buffer(
    const char* attribute, void** buffer, uint64_t** buffer_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(attribute, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized != constants::coords) {
    if (array_schema->attribute(normalized) == nullptr)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot get buffer; Invalid attribute name '") +
          normalized + "'"));
  }
  if (array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Attribute '") + normalized +
        "' is var-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(normalized, buffer, buffer_size);
  return reader_.get_buffer(normalized, buffer, buffer_size);
}

Status Query::get_buffer(
    const char* attribute,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(attribute, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized == constants::coords) {
    return LOG_STATUS(
        Status::QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema->attribute(normalized) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") +
        normalized + "'"));
  if (!array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Attribute '") + normalized +
        "' is fixed-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(
        normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.get_buffer(
      normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
}

bool Query::has_results() const {
  if (status_ == QueryStatus::UNINITIALIZED || type_ == QueryType::WRITE)
    return false;
  return !reader_.no_results();
}

Status Query::init() {
  // Only if the query has not been initialized before
  if (status_ == QueryStatus::UNINITIALIZED) {
    // Check if the array got closed
    if (array_ == nullptr || !array_->is_open())
      return LOG_STATUS(Status::QueryError(
          "Cannot init query; The associated array is not open"));

    // Check if the array got re-opened with a different query type
    QueryType array_query_type;
    RETURN_NOT_OK(array_->get_query_type(&array_query_type));
    if (array_query_type != type_) {
      std::stringstream errmsg;
      errmsg << "Cannot init query; "
             << "Associated array query type does not match query type: "
             << "(" << query_type_str(array_query_type)
             << " != " << query_type_str(type_) << ")";
      return LOG_STATUS(Status::QueryError(errmsg.str()));
    }

    if (type_ == QueryType::READ) {
      RETURN_NOT_OK(reader_.init());
    } else {  // Write
      RETURN_NOT_OK(writer_.init());
    }
  }

  status_ = QueryStatus::INPROGRESS;

  return Status::Ok();
}

URI Query::last_fragment_uri() const {
  if (type_ == QueryType::WRITE)
    return URI();
  return reader_.last_fragment_uri();
}

Layout Query::layout() const {
  return layout_;
}

Status Query::cancel() {
  status_ = QueryStatus::FAILED;
  return Status::Ok();
}

Status Query::check_var_attr_offsets(
    const uint64_t* buffer_off,
    const uint64_t* buffer_off_size,
    const uint64_t* buffer_val_size) {
  if (buffer_off == nullptr || buffer_off_size == nullptr ||
      buffer_val_size == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot use null offset buffers."));

  auto num_offsets = *buffer_off_size / sizeof(uint64_t);
  if (num_offsets == 0)
    return Status::Ok();

  uint64_t prev_offset = buffer_off[0];
  if (prev_offset >= *buffer_val_size)
    return LOG_STATUS(Status::QueryError(
        "Invalid offsets; offset " + std::to_string(prev_offset) +
        " specified for buffer of size " + std::to_string(*buffer_val_size)));

  for (uint64_t i = 1; i < num_offsets; i++) {
    if (buffer_off[i] <= prev_offset)
      return LOG_STATUS(
          Status::QueryError("Invalid offsets; offsets must be given in "
                             "strictly ascending order."));

    if (buffer_off[i] >= *buffer_val_size)
      return LOG_STATUS(Status::QueryError(
          "Invalid offsets; offset " + std::to_string(buffer_off[i]) +
          " specified for buffer of size " + std::to_string(*buffer_val_size)));

    prev_offset = buffer_off[i];
  }

  return Status::Ok();
}

Status Query::capnp(rest::capnp::Query::Builder* queryBuilder) {
  STATS_FUNC_IN(serialization_query_capnp);

  if (layout_ == Layout::GLOBAL_ORDER)
    return LOG_STATUS(Status::QueryError(
        "Cannot serialize; global order serialization not supported."));

  const auto* schema = array_schema();
  if (schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot serialize; array schema is null."));

  const auto* domain = schema->domain();
  if (domain == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot serialize; array domain is null."));

  // Serialize basic fields
  queryBuilder->setType(query_type_str(type_));
  queryBuilder->setLayout(layout_str(layout_));
  queryBuilder->setStatus(query_status_str(status_));

  // Serialize array
  if (array_ != nullptr) {
    auto builder = queryBuilder->initArray();
    RETURN_NOT_OK(array_->capnp(&builder));
  }

  // Serialize subarray
  const void* subarray =
      type_ == QueryType::READ ? reader_.subarray() : writer_.subarray();
  if (subarray != nullptr) {
    auto subarray_builder = queryBuilder->initSubarray();
    RETURN_NOT_OK(rest::capnp::utils::serialize_subarray(
        subarray_builder, schema, subarray));
  }

  // Serialize attribute buffer metadata
  const auto attr_names = attributes();
  auto attr_buffers_builder =
      queryBuilder->initAttributeBufferHeaders(attr_names.size());
  uint64_t total_fixed_len_bytes = 0;
  uint64_t total_var_len_bytes = 0;
  for (uint64_t i = 0; i < attr_names.size(); i++) {
    auto attr_buffer_builder = attr_buffers_builder[i];
    const auto& attribute_name = attr_names[i];
    const auto& buff = type_ == QueryType::READ ?
                           reader_.buffer(attribute_name) :
                           writer_.buffer(attribute_name);
    const bool is_coords = attribute_name == constants::coords;
    const auto* attr = schema->attribute(attribute_name);
    if (!is_coords && attr == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Cannot serialize; no attribute named '" + attribute_name + "'."));

    const bool var_size = !is_coords && attr->var_size();
    attr_buffer_builder.setName(attribute_name);
    if (var_size &&
        (buff.buffer_var_ != nullptr && buff.buffer_var_size_ != nullptr)) {
      // Variable-sized attribute.
      if (buff.buffer_ == nullptr || buff.buffer_size_ == nullptr)
        return LOG_STATUS(Status::QueryError(
            "Cannot serialize; no offset buffer set for attribute '" +
            attribute_name + "'."));
      total_var_len_bytes += *buff.buffer_var_size_;
      attr_buffer_builder.setVarLenBufferSizeInBytes(*buff.buffer_var_size_);
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);
    } else if (buff.buffer_ != nullptr && buff.buffer_size_ != nullptr) {
      // Fixed-length attribute
      total_fixed_len_bytes += *buff.buffer_size_;
      attr_buffer_builder.setFixedLenBufferSizeInBytes(*buff.buffer_size_);
      attr_buffer_builder.setVarLenBufferSizeInBytes(0);
    }
  }

  queryBuilder->setTotalFixedLengthBufferBytes(total_fixed_len_bytes);
  queryBuilder->setTotalVarLenBufferBytes(total_var_len_bytes);

  if (type_ == QueryType::READ) {
    auto builder = queryBuilder->initReader();
    RETURN_NOT_OK(reader_.capnp(&builder));
  } else {
    auto builder = queryBuilder->initWriter();
    RETURN_NOT_OK(writer_.capnp(&builder));
  }

  return Status::Ok();

  STATS_FUNC_OUT(serialization_query_capnp);
}

tiledb::sm::Status Query::from_capnp(
    bool clientside, rest::capnp::Query::Reader* query, void* buffer_start) {
  STATS_FUNC_IN(serialization_query_from_capnp);

  const auto* schema = array_schema();
  if (schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot deserialize; array schema is null."));

  const auto* domain = schema->domain();
  if (domain == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot deserialize; array domain is null."));

  if (array_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot deserialize; array pointer is null."));

  // Deserialize query type (sanity check).
  QueryType query_type = QueryType::READ;
  RETURN_NOT_OK(query_type_enum(query->getType().cStr(), &query_type));
  if (query_type != type_)
    return LOG_STATUS(Status::QueryError(
        "Cannot deserialize; Query opened for " + query_type_str(type_) +
        " but got serialized type for " + query->getType().cStr()));

  // Deserialize layout.
  Layout layout = Layout::UNORDERED;
  RETURN_NOT_OK(layout_enum(query->getLayout().cStr(), &layout));
  RETURN_NOT_OK(set_layout(layout));

  // Deserialize array instance.
  RETURN_NOT_OK(array_->from_capnp(query->getArray()));

  // Deserialize and set subarray.
  const bool sparse_write = !schema->dense() || layout_ == Layout::UNORDERED;
  if (sparse_write) {
    // Sparse writes cannot have a subarray; clear it here.
    RETURN_NOT_OK(set_subarray(nullptr));
  } else {
    auto subarray_reader = query->getSubarray();
    void* subarray;
    RETURN_NOT_OK(rest::capnp::utils::deserialize_subarray(
        subarray_reader, schema, &subarray));
    RETURN_NOT_OK_ELSE(set_subarray(subarray), std::free(subarray));
    std::free(subarray);
  }

  // Deserialize and set attribute buffers.
  if (!query->hasAttributeBufferHeaders())
    return LOG_STATUS(Status::QueryError(
        "Cannot deserialize; no attribute buffer headers in message."));

  auto buffer_headers = query->getAttributeBufferHeaders();
  auto attribute_buffer_start = static_cast<char*>(buffer_start);
  for (auto buffer_header : buffer_headers) {
    const std::string attribute_name = buffer_header.getName().cStr();
    const bool is_coords = attribute_name == constants::coords;
    const auto* attr = schema->attribute(attribute_name);
    if (!is_coords && attr == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Cannot deserialize; no attribute named '" + attribute_name +
          "' in array schema."));

    // Get buffer sizes required
    const uint64_t fixedlen_size = buffer_header.getFixedLenBufferSizeInBytes();
    const uint64_t varlen_size = buffer_header.getVarLenBufferSizeInBytes();

    // Get any buffers already set on this query object.
    uint64_t* existing_offset_buffer = nullptr;
    uint64_t* existing_offset_buffer_size = nullptr;
    void* existing_buffer = nullptr;
    uint64_t* existing_buffer_size = nullptr;
    const bool var_size = !is_coords && attr->var_size();
    if (var_size) {
      RETURN_NOT_OK(get_buffer(
          attribute_name.c_str(),
          &existing_offset_buffer,
          &existing_offset_buffer_size,
          &existing_buffer,
          &existing_buffer_size));
    } else {
      RETURN_NOT_OK(get_buffer(
          attribute_name.c_str(), &existing_buffer, &existing_buffer_size));
    }

    if (clientside) {
      // For queries on the client side, we require that buffers have been
      // set by the user, and that they are large enough for all the serialized
      // data.
      const bool null_buffer =
          existing_buffer == nullptr || existing_buffer_size == nullptr;
      const bool null_offset_buffer = existing_offset_buffer == nullptr ||
                                      existing_offset_buffer_size == nullptr;
      if ((var_size && (null_buffer || null_offset_buffer)) ||
          (!var_size && null_buffer))
        return LOG_STATUS(Status::QueryError(
            "Error deserializing read query; buffer not set for attribute '" +
            attribute_name + "'."));
      if ((var_size && (*existing_offset_buffer_size < fixedlen_size ||
                        *existing_buffer_size < varlen_size)) ||
          (!var_size && *existing_buffer_size < fixedlen_size)) {
        return LOG_STATUS(Status::QueryError(
            "Error deserializing read query; buffer too small for attribute "
            "'" +
            attribute_name + "'."));
      }

      // For reads, copy the response data into user buffers. For writes,
      // nothing to do.
      if (type_ == QueryType::READ) {
        if (var_size) {
          // Var size attribute; buffers already set.
          std::memcpy(
              existing_offset_buffer, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;
          std::memcpy(existing_buffer, attribute_buffer_start, varlen_size);
          attribute_buffer_start += varlen_size;

          // Need to update the buffer size to the actual data size so that
          // the user can check the result size on reads.
          *existing_offset_buffer_size = fixedlen_size;
          *existing_buffer_size = varlen_size;
        } else {
          // Fixed size attribute; buffers already set.
          std::memcpy(existing_buffer, attribute_buffer_start, fixedlen_size);
          attribute_buffer_start += fixedlen_size;

          // Need to update the buffer size to the actual data size so that
          // the user can check the result size on reads.
          *existing_buffer_size = fixedlen_size;
        }
      }
    } else {
      // Server-side; always expect null buffers when deserializing.
      if (existing_buffer != nullptr || existing_offset_buffer != nullptr)
        return LOG_STATUS(
            Status::QueryError("Error deserializing read query; unexpected "
                               "buffer set on server-side."));

      auto& attr_state = serialization_state_.attribute_states[attribute_name];
      if (type_ == QueryType::READ) {
        // On reads, just set null pointers with accurate size so that the
        // server can introspect and allocate properly sized buffers separately.
        Buffer offsets_buff(nullptr, fixedlen_size, false);
        Buffer varlen_buff(nullptr, varlen_size, false);
        attr_state.fixed_len_size = fixedlen_size;
        attr_state.var_len_size = varlen_size;
        attr_state.fixed_len_data.swap(offsets_buff);
        attr_state.var_len_data.swap(varlen_buff);
        if (var_size) {
          RETURN_NOT_OK(set_buffer(
              attribute_name,
              nullptr,
              &attr_state.fixed_len_size,
              nullptr,
              &attr_state.var_len_size,
              false));
        } else {
          RETURN_NOT_OK(set_buffer(
              attribute_name, nullptr, &attr_state.fixed_len_size, false));
        }
      } else {
        // On writes, just set buffer pointers wrapping the data in the message.
        if (var_size) {
          auto* offsets = reinterpret_cast<uint64_t*>(attribute_buffer_start);
          auto* varlen_data = attribute_buffer_start + fixedlen_size;
          attribute_buffer_start += fixedlen_size + varlen_size;
          Buffer offsets_buff(offsets, fixedlen_size, false);
          Buffer varlen_buff(varlen_data, varlen_size, false);
          attr_state.fixed_len_size = fixedlen_size;
          attr_state.var_len_size = varlen_size;
          attr_state.fixed_len_data.swap(offsets_buff);
          attr_state.var_len_data.swap(varlen_buff);
          RETURN_NOT_OK(set_buffer(
              attribute_name,
              offsets,
              &attr_state.fixed_len_size,
              varlen_data,
              &attr_state.var_len_size));
        } else {
          auto* data = attribute_buffer_start;
          attribute_buffer_start += fixedlen_size;
          Buffer buff(data, fixedlen_size, false);
          Buffer varlen_buff(nullptr, 0, false);
          attr_state.fixed_len_size = fixedlen_size;
          attr_state.var_len_size = varlen_size;
          attr_state.fixed_len_data.swap(buff);
          attr_state.var_len_data.swap(varlen_buff);
          RETURN_NOT_OK(
              set_buffer(attribute_name, data, &attr_state.fixed_len_size));
        }
      }
    }
  }

  // Deserialize reader/writer.
  if (type_ == QueryType::READ) {
    auto reader = query->getReader();
    RETURN_NOT_OK(reader_.from_capnp(&reader));
  } else {
    auto writer = query->getWriter();
    RETURN_NOT_OK(writer_.from_capnp(&writer));
  }

  // Deserialize status. This must come last because various setters above
  // will reset it.
  QueryStatus query_status = QueryStatus::UNINITIALIZED;
  RETURN_NOT_OK(query_status_enum(query->getStatus().cStr(), &query_status));
  set_status(query_status);

  return Status::Ok();

  STATS_FUNC_OUT(serialization_query_from_capnp);
}

Status Query::process() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return LOG_STATUS(
        Status::QueryError("Cannot process query; Query is not initialized"));
  status_ = QueryStatus::INPROGRESS;

  // Process query
  Status st = Status::Ok();
  if (type_ == QueryType::READ)
    st = reader_.read();
  else  // WRITE MODE
    st = writer_.write();

  // Handle error
  if (!st.ok()) {
    status_ = QueryStatus::FAILED;
    return st;
  }

  // Check if the query is complete
  bool completed = (type_ == QueryType::WRITE) ? true : !reader_.incomplete();

  // Handle callback and status
  if (completed) {
    if (callback_ != nullptr)
      callback_(callback_data_);
    status_ = QueryStatus::COMPLETED;
  } else {  // Incomplete
    status_ = QueryStatus::INCOMPLETE;
  }

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& attribute,
    void* buffer,
    uint64_t* buffer_size,
    bool check_null_buffers) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(attribute, buffer, buffer_size);
  return reader_.set_buffer(attribute, buffer, buffer_size, check_null_buffers);
}

Status Query::set_buffer(
    const std::string& attribute,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size,
    bool check_null_buffers) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(
        attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.set_buffer(
      attribute,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      check_null_buffers);
}

Status Query::set_layout(Layout layout) {
  layout_ = layout;
  if (type_ == QueryType::WRITE)
    return writer_.set_layout(layout);
  return reader_.set_layout(layout);
}

Status Query::set_sparse_mode(bool sparse_mode) {
  if (type_ != QueryType::READ)
    return LOG_STATUS(Status::QueryError(
        "Cannot set sparse mode; Only applicable to read queries"));

  return reader_.set_sparse_mode(sparse_mode);
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

Status Query::set_subarray(const void* subarray) {
  RETURN_NOT_OK(check_subarray(subarray));
  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::set_subarray(const Subarray& subarray) {
  // Check that the subarray is associated with the same array as the query
  if (subarray.array() != array_)
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; The array of subarray is "
                           "different from that of the query"));

  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::submit() {  // Do nothing if the query is completed or failed
  if (array_->is_remote()) {
    array_->array_schema()->set_array_uri(array_->array_uri());
    Config config = this->storage_manager_->config();
    return tiledb::rest::submit_query_to_rest(
        config, array_->array_uri().to_string(), this);
  }
  RETURN_NOT_OK(init());
  return storage_manager_->query_submit(this);
}

Status Query::submit_async(
    std::function<void(void*)> callback, void* callback_data) {
  RETURN_NOT_OK(init());
  if (array_->is_remote())
    return LOG_STATUS(
        Status::QueryError("Error in async query submission; async queries not "
                           "supported for remote arrays."));

  callback_ = callback;
  callback_data_ = callback_data;
  return storage_manager_->query_submit_async(this);
}

QueryStatus Query::status() const {
  return status_;
}

QueryType Query::type() const {
  return type_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Query::check_subarray(const void* subarray) const {
  if (subarray == nullptr)
    return Status::Ok();

  auto array_schema = this->array_schema();
  if (array_schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot check subarray; Array schema not set"));

  switch (array_schema->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return check_subarray<uint8_t>(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return check_subarray<int16_t>(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return check_subarray<uint16_t>(static_cast<const uint16_t*>(subarray));
    case Datatype::INT32:
      return check_subarray<int32_t>(static_cast<const int32_t*>(subarray));
    case Datatype::UINT32:
      return check_subarray<uint32_t>(static_cast<const uint32_t*>(subarray));
    case Datatype::INT64:
      return check_subarray<int64_t>(static_cast<const int64_t*>(subarray));
    case Datatype::UINT64:
      return check_subarray<uint64_t>(static_cast<const uint64_t*>(subarray));
    case Datatype::FLOAT32:
      return check_subarray<float>(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return check_subarray<double>(static_cast<const double*>(subarray));
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      break;
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
