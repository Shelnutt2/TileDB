/**
 * @file   file_schema.cc
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
 * This file implements the FileSchema class.
 */

#include "tiledb/sm/file/file_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/layout.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

FileSchema::FileSchema()
    : ArraySchema(ArrayType::DENSE) {
  allows_dups_ = false;
  array_uri_ = URI();
  uri_ = URI();
  name_ = "";
  capacity_ = constants::capacity;
  cell_order_ = Layout::ROW_MAJOR;
  tile_order_ = Layout::ROW_MAJOR;

  // Set domain
  tdb_delete(domain_);
  domain_ = tdb_new(Domain, create_default_domain());

  // Create dimension map
  dim_map_.clear();
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = dimension(d);
    dim_map_[dim->name()] = dim;

    version_ = constants::format_version;
    auto timestamp = utils::time::timestamp_now_ms();
    timestamp_range_ = std::make_pair(timestamp, timestamp);

    // Set up default filter pipelines for coords, offsets, and validity values.
    coords_filters_.add_filter(CompressionFilter(
        constants::coords_compression, constants::coords_compression_level));
    cell_var_offsets_filters_.add_filter(CompressionFilter(
        constants::cell_var_offsets_compression,
        constants::cell_var_offsets_compression_level));
    cell_validity_filters_.add_filter(CompressionFilter(
        constants::cell_validity_compression,
        constants::cell_validity_compression_level));
  }
}

FileSchema::FileSchema(const FileSchema* file_schema)
    : ArraySchema(file_schema) {
  /*allows_dups_ = array_schema->allows_dups_;
  array_uri_ = array_schema->array_uri_;
  uri_ = array_schema->uri_;
  name_ = array_schema->name_;
  array_type_ = array_schema->array_type_;
  domain_ = nullptr;
  timestamp_range_ = array_schema->timestamp_range_;

  capacity_ = array_schema->capacity_;
  cell_order_ = array_schema->cell_order_;
  cell_var_offsets_filters_ = array_schema->cell_var_offsets_filters_;
  cell_validity_filters_ = array_schema->cell_validity_filters_;
  coords_filters_ = array_schema->coords_filters_;
  tile_order_ = array_schema->tile_order_;
  version_ = array_schema->version_;

  set_domain(array_schema->domain_);

  attribute_map_.clear();
  for (auto attr : array_schema->attributes_)
    add_attribute(attr, false);*/
}

FileSchema::~FileSchema() {
  //  clear();
}

/* ****************************** */
/*               API              */
/* ****************************** */

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

Domain FileSchema::create_default_domain() {
  Domain domain;

  domain.add_dimension();

  return domain;
}

}  // namespace sm
}  // namespace tiledb
