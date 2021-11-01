/**
 * @file   file_schema.h
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
 * This file defines class FileSchema.
 */

#ifndef TILEDB_FILE_SCHEMA_H
#define TILEDB_FILE_SCHEMA_H

#include "tiledb/sm/array_schema/array_schema.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

// class Attribute;
// class Buffer;
// class ConstBuffer;
// class Dimension;
// class Domain;
//
enum class ArrayType : uint8_t;
// enum class Compressor : uint8_t;
// enum class Datatype : uint8_t;
// enum class Layout : uint8_t;

/** Specifies the file array schema. */
class FileSchema : public ArraySchema {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  FileSchema();

  /** Constructor. */
  FileSchema(ArrayType array_type);

  /**
   * Constructor. Clones the input.
   *
   * @param array_schema The array schema to copy.
   */
  explicit FileSchema(const FileSchema* file_schema);

  /** Destructor. */
  ~FileSchema();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Domain create_default_domain();

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILE_SCHEMA_H
