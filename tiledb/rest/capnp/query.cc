/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines serialization for
 * tiledb::sm::Query
 */

#include "tiledb/rest/capnp/query.h"
#include "capnp/compat/json.h"
#include "capnp/message.h"
#include "capnp/serialize.h"
#include "tiledb/rest/capnp/utils.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace rest {
namespace capnp {

tiledb::sm::Status query_serialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    tiledb::sm::Buffer* serialized_buffer) {
  STATS_FUNC_IN(serialization_query_serialize);

  try {
    ::capnp::MallocMessageBuilder message;
    Query::Builder query_builder = message.initRoot<Query>();
    RETURN_NOT_OK(query->capnp(&query_builder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    uint64_t total_fixed_len_bytes =
        query_builder.getTotalFixedLengthBufferBytes();
    uint64_t total_var_len_bytes = query_builder.getTotalVarLenBufferBytes();
    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(query_builder);
        const auto json_len = capnp_json.size();
        const char nul = '\0';
        // size does not include needed null terminator, so add +1
        RETURN_NOT_OK(serialized_buffer->realloc(json_len + 1));
        RETURN_NOT_OK(serialized_buffer->write(capnp_json.cStr(), json_len))
        RETURN_NOT_OK(serialized_buffer->write(&nul, 1));
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        kj::Array<::capnp::word> protomessage = messageToFlatArray(message);
        kj::ArrayPtr<const char> message_chars = protomessage.asChars();

        // Write the serialized query
        const auto total_nbytes =
            message_chars.size() + total_fixed_len_bytes + total_var_len_bytes;
        RETURN_NOT_OK(serialized_buffer->realloc(total_nbytes));
        RETURN_NOT_OK(serialized_buffer->write(
            message_chars.begin(), message_chars.size()));

        const auto* array_schema = query->array_schema();
        if (array_schema == nullptr)
          return LOG_STATUS(tiledb::sm::Status::QueryError(
              "Cannot serialize; array schema is null."));

        // Iterate in attributes and concatenate buffers to end of message
        auto attr_buffer_builders = query_builder.getAttributeBufferHeaders();
        for (auto attr_buffer_builder : attr_buffer_builders) {
          std::string attribute_name = attr_buffer_builder.getName().cStr();
          const auto* attr = array_schema->attribute(attribute_name);
          if (attr == nullptr)
            return LOG_STATUS(tiledb::sm::Status::QueryError(
                "Cannot serialize; no attribute named '" + attribute_name +
                "'."));

          if (attr->var_size()) {
            // Variable size attribute buffer
            uint64_t* offset_buffer = nullptr;
            uint64_t* offset_buffer_size = nullptr;
            void* buffer = nullptr;
            uint64_t* buffer_size = nullptr;
            RETURN_NOT_OK(query->get_buffer(
                attribute_name.c_str(),
                &offset_buffer,
                &offset_buffer_size,
                &buffer,
                &buffer_size));
            RETURN_NOT_OK(
                serialized_buffer->write(offset_buffer, *offset_buffer_size));
            RETURN_NOT_OK(serialized_buffer->write(buffer, *buffer_size));
          } else {
            // Fixed size attribute buffer
            void* buffer = nullptr;
            uint64_t* buffer_size = nullptr;
            RETURN_NOT_OK(query->get_buffer(
                attribute_name.c_str(), &buffer, &buffer_size));
            RETURN_NOT_OK(serialized_buffer->write(buffer, *buffer_size));
          }
        }
        break;
      }
      default:
        return LOG_STATUS(tiledb::sm::Status::QueryError(
            "Cannot serialize; unknown serialization type"));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(tiledb::sm::Status::QueryError(
        "Cannot serialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(tiledb::sm::Status::QueryError(
        "Cannot serialize; exception: " + std::string(e.what())));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_query_serialize);
}

tiledb::sm::Status query_deserialize(
    tiledb::sm::Query* query,
    tiledb::sm::SerializationType serialize_type,
    const tiledb::sm::Buffer& serialized_buffer) {
  STATS_FUNC_IN(serialization_query_deserialize);

  try {
    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        rest::capnp::Query::Builder query_builder =
            message_builder.initRoot<rest::capnp::Query>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            query_builder);
        rest::capnp::Query::Reader query_reader = query_builder.asReader();
        return query->from_capnp(&query_reader, nullptr);
      }
      case tiledb::sm::SerializationType::CAPNP: {
        // Capnp FlatArrayMessageReader requires 64-bit alignment.
        if (!utils::is_aligned<sizeof(uint64_t)>(serialized_buffer.data()))
          return LOG_STATUS(tiledb::sm::Status::RestError(
              "Could not deserialize query; buffer is not 8-byte aligned."));

        // Set traversal limit to 10GI (TODO: make this a config option)
        ::capnp::ReaderOptions readerOptions;
        readerOptions.traversalLimitInWords = uint64_t(1024) * 1024 * 1024 * 10;
        ::capnp::FlatArrayMessageReader reader(
            kj::arrayPtr(
                reinterpret_cast<const ::capnp::word*>(
                    serialized_buffer.data()),
                serialized_buffer.size() / sizeof(::capnp::word)),
            readerOptions);

        Query::Reader query_reader = reader.getRoot<rest::capnp::Query>();

        // Get a pointer to the start of the attribute buffer data (which
        // was concatenated after the CapnP message on serialization).
        auto attribute_buffer_start = reader.getEnd();
        auto buffer_start = const_cast<::capnp::word*>(attribute_buffer_start);
        return query->from_capnp(&query_reader, buffer_start);
      }
      default:
        return LOG_STATUS(tiledb::sm::Status::QueryError(
            "Cannot deserialize; unknown serialization type."));
    }
  } catch (kj::Exception& e) {
    return LOG_STATUS(tiledb::sm::Status::QueryError(
        "Cannot deserialize; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(tiledb::sm::Status::QueryError(
        "Cannot deserialize; exception: " + std::string(e.what())));
  }
  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_query_deserialize);
}

}  // namespace capnp
}  // namespace rest
}  // namespace tiledb
