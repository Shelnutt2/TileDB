/**
 * @file   vector_serializer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines a VectorSerializer class.
 */

#ifndef TILEDB_VECTOR_SERIALIZER_H
#define TILEDB_VECTOR_SERIALIZER_H

#include <vector>
#include "tiledb/common/status.h"

namespace tiledb::sm {

class VectorSerializer {
 public:
  /**
   * Constructor.
   *
   * @param buffer The vector to serialize to.
   */
  VectorSerializer(std::vector<uint8_t>& buffer)
      : buffer_(buffer) {
  }

  /**
   * Writes the given data to the buffer.
   *
   * @param data The data to write.
   * @param size The size of the data to write.
   * @return Status
   */
  Status write(const void* data, const size_t size) {
    if (size == 0) {
      return Status::Ok();
    }
    const auto old_size = buffer_.size();
    buffer_.resize(old_size + size);
    std::memcpy(buffer_.data() + old_size, data, size);
    return Status::Ok();
  }

 private:
  /** The buffer to write to. */
  std::vector<uint8_t>& buffer_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_VECTOR_SERIALIZER_H
