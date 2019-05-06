/**
 * @file   serialization.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file declares the C++ API for TileDB serialization interface.
 */

#ifndef TILEDB_CPP_API_SERIALIZATION_H
#define TILEDB_CPP_API_SERIALIZATION_H

#include "array.h"
#include "context.h"
#include "deleter.h"
#include "query.h"
#include "tiledb.h"
#include "type.h"

#include <memory>

namespace tiledb {

/**
 * Class wrapping the C API for serialization.
 */
class Serialization {
 public:
  /**
   * Serializes the given query into the given buffer.
   *
   * @param query Query to serialize
   * @param buffer Vector that will store a copy of the serialized bytes.
   */
  static void serialize_query(
      const Context& ctx, const Query& query, std::vector<uint8_t>* buffer) {
    tiledb_buffer_t* c_buff;
    ctx.handle_error(tiledb_buffer_alloc(ctx, &c_buff));

    // Wrap in a safe pointer
    auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
    std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(
        c_buff, deleter);

    // Serialize
    ctx.handle_error(
        tiledb_serialize_query(ctx, query.query_.get(), TILEDB_CAPNP, c_buff));

    // Copy into user vector
    void* data;
    uint64_t num_bytes;
    ctx.handle_error(tiledb_buffer_get_data(ctx, c_buff, &data, &num_bytes));
    buffer->clear();
    buffer->insert(
        buffer->end(),
        static_cast<const uint8_t*>(data),
        static_cast<const uint8_t*>(data) + num_bytes);
  }

  /**
   * Deserializes the given buffer into the given query.
   *
   * @note because TileDB serialization uses zero-copy when possible, the given
   * buffer should not be freed/modified until the deserialized query has been
   * destroyed.
   *
   * @param buffer Vector holding the serialized query bytes.
   * @param query Query to deserialize into
   */
  static void deserialize_query(
      const Context& ctx, std::vector<uint8_t>& buffer, Query* query) {
    tiledb_buffer_t* c_buff;
    ctx.handle_error(tiledb_buffer_alloc(ctx, &c_buff));

    // Wrap in a safe pointer
    auto deleter = [](tiledb_buffer_t* b) { tiledb_buffer_free(&b); };
    std::unique_ptr<tiledb_buffer_t, decltype(deleter)> buff_ptr(
        c_buff, deleter);

    // Deserialize
    ctx.handle_error(tiledb_buffer_set_data(
        ctx,
        c_buff,
        reinterpret_cast<void*>(&buffer[0]),
        static_cast<uint64_t>(buffer.size())));
    ctx.handle_error(tiledb_deserialize_query(
        ctx, query->query_.get(), TILEDB_CAPNP, c_buff));
  }
};

}  // namespace tiledb

#endif  // TILEDB_CPP_API_SERIALIZATION_H
