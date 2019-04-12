/**
 * @file unit-capi-capnp.cc
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
 * Tests for the C API tiledb_array_schema_t spec, along with
 * tiledb_attribute_iter_t and tiledb_dimension_iter_t.
 */

#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>
#include "tiledb/sm/enums/serialization_type.h"

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <chrono>  // for high_resolution_clock

struct ArraySchemaCapnp {
// Filesystem related
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif

  // Constant parameters
  const std::string ARRAY_NAME = "dense_test_100x100_10x10";
  tiledb_array_type_t ARRAY_TYPE = TILEDB_DENSE;
  const char* ARRAY_TYPE_STR = "dense";
  const uint64_t CAPACITY = 500;
  const char* CAPACITY_STR = "500";
  const tiledb_layout_t CELL_ORDER = TILEDB_COL_MAJOR;
  const char* CELL_ORDER_STR = "col-major";
  const tiledb_layout_t TILE_ORDER = TILEDB_ROW_MAJOR;
  const char* TILE_ORDER_STR = "row-major";
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* ATTR_TYPE_STR = "INT32";
  const tiledb_filter_type_t ATTR_COMPRESSOR = TILEDB_FILTER_NONE;
  const char* ATTR_COMPRESSOR_STR = "NO_COMPRESSION";
  const int ATTR_COMPRESSION_LEVEL = -1;
  const char* ATTR_COMPRESSION_LEVEL_STR = "-1";
  const unsigned int CELL_VAL_NUM = 1;
  const char* CELL_VAL_NUM_STR = "1";
  const int DIM_NUM = 2;
  const char* DIM1_NAME = "d1";
  const char* DIM2_NAME = "d2";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const char* DIM_TYPE_STR = "INT64";
  const int64_t DIM_DOMAIN[4] = {0, 99, 20, 60};
  const char* DIM1_DOMAIN_STR = "[0,99]";
  const char* DIM2_DOMAIN_STR = "[20,60]";
  const uint64_t DIM_DOMAIN_SIZE = sizeof(DIM_DOMAIN) / DIM_NUM;
  const int64_t TILE_EXTENTS[2] = {10, 5};
  const char* DIM1_TILE_EXTENT_STR = "10";
  const char* DIM2_TILE_EXTENT_STR = "5";
  const uint64_t TILE_EXTENT_SIZE = sizeof(TILE_EXTENTS) / DIM_NUM;
  const char* REST_SERVER = "http://localhost:8080";

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  ArraySchemaCapnp();
  ~ArraySchemaCapnp();
  void set_supported_fs();
  tiledb_array_schema_t* create_array_schema();
  tiledb_array_schema_t* create_array_schema_sparse();
  tiledb_array_schema_t* create_array_schema_simple();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
};

ArraySchemaCapnp::ArraySchemaCapnp() {
  // Supported filesystems
  set_supported_fs();
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config,
          "rest.server_serialization_format",
          tiledb::sm::serialization_type_str(
              tiledb::sm::SerializationType::CAPNP)
              .c_str(),
          &error) == TILEDB_OK);

  // tiledb_ctx_free(&ctx_);
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);

  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
  tiledb_config_free(&config);
}

ArraySchemaCapnp::~ArraySchemaCapnp() {
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void ArraySchemaCapnp::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  tiledb_ctx_free(&ctx);
}

tiledb_array_schema_t* ArraySchemaCapnp::create_array_schema() {
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "", ATTR_TYPE, &attr1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_alloc(ctx_, "a1", ATTR_TYPE, &attr2);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr1);
  tiledb_attribute_free(&attr2);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);

  return array_schema;
}

tiledb_array_schema_t* ArraySchemaCapnp::create_array_schema_sparse() {
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "", ATTR_TYPE, &attr1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* attr2;
  rc = tiledb_attribute_alloc(ctx_, "a1", ATTR_TYPE, &attr2);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr2);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr1);
  tiledb_attribute_free(&attr2);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);

  return array_schema;
}

tiledb_array_schema_t* ArraySchemaCapnp::create_array_schema_simple() {
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, &DIM_DOMAIN[0], &TILE_EXTENTS[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "a1", ATTR_TYPE, &attr1);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);

  tiledb_attribute_free(&attr1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);

  return array_schema;
}

void ArraySchemaCapnp::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ArraySchemaCapnp::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

#ifdef TILEDB_SERIALIZATION

TEST_CASE_METHOD(
    ArraySchemaCapnp,
    "C API: Test array schema capnp serialization",
    "[capi], [capnp]") {
  // Create array schema
  tiledb_array_schema_t* array_schema = create_array_schema();
  // Create array schema to post

  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string uri = temp_dir + "capnp_serialization_test";

  // Load array schema from the rest server
  tiledb_array_schema_t* array_schema_returned;
  char* data;
  uint64_t data_size = 0;
  int rc = tiledb_array_schema_serialize(
      ctx_,
      array_schema,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      &data,
      &data_size);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_deserialize(
      ctx_,
      &array_schema_returned,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      data,
      data_size);
  REQUIRE(rc == TILEDB_OK);

  // Layout should be the same
  tiledb_layout_t layout, layout_returned;
  rc = tiledb_array_schema_get_cell_order(ctx_, array_schema, &layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_cell_order(
      ctx_, array_schema_returned, &layout_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == layout_returned);

  // Capacity should be the same
  uint64_t cap, cap_returned;
  rc = tiledb_array_schema_get_capacity(ctx_, array_schema, &cap);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_capacity(
      ctx_, array_schema_returned, &cap_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(cap == cap_returned);

  // Two attributes were created, a named and an anonymous
  // Anonymous attribute cannot make it to the other side
  // since it gets a default name (constants::default_attr_name)
  // which is prefixed (__attr)
  uint32_t attr_num, attr_num_returned;
  rc = tiledb_array_schema_get_attribute_num(ctx_, array_schema, &attr_num);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_attribute_num(
      ctx_, array_schema_returned, &attr_num_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(attr_num == attr_num_returned);

  // Retrieve domains
  tiledb_domain_t* domain;
  tiledb_array_schema_get_domain(ctx_, array_schema, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_t* domain_returned;
  tiledb_array_schema_get_domain(ctx_, array_schema_returned, &domain_returned);
  REQUIRE(rc == TILEDB_OK);

  // Number of dimensions should be the same
  uint32_t ndim, ndim_returned;
  rc = tiledb_domain_get_ndim(ctx_, domain, &ndim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_get_ndim(ctx_, domain_returned, &ndim_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(ndim == ndim_returned);

  // Check if there is an anonymous dimension
  tiledb_dimension_t* dim;
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "", &dim);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_returned;
  rc = tiledb_domain_get_dimension_from_name(
      ctx_, domain_returned, "", &dim_returned);
  REQUIRE(rc == TILEDB_OK);

  tiledb_array_schema_free(&array_schema);
  tiledb_array_schema_free(&array_schema_returned);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArraySchemaCapnp,
    "C API: Test sparse array schema capnp serialization",
    "[capi], [capnp]") {
  // Create array schema
  tiledb_array_schema_t* array_schema = create_array_schema_sparse();
  // Create array schema to post

  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);

  // Create and write sparse array
  std::string uri = temp_dir + "capnp_serialization_sparse_test";

  // Load array schema from the rest server
  tiledb_array_schema_t* array_schema_returned;
  char* data;
  uint64_t data_size = 0;
  int rc = tiledb_array_schema_serialize(
      ctx_,
      array_schema,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      &data,
      &data_size);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_deserialize(
      ctx_,
      &array_schema_returned,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      data,
      data_size);
  REQUIRE(rc == TILEDB_OK);

  // Layout should be the same
  tiledb_layout_t layout, layout_returned;
  rc = tiledb_array_schema_get_cell_order(ctx_, array_schema, &layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_cell_order(
      ctx_, array_schema_returned, &layout_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == layout_returned);

  // Capacity should be the same
  uint64_t cap, cap_returned;
  rc = tiledb_array_schema_get_capacity(ctx_, array_schema, &cap);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_capacity(
      ctx_, array_schema_returned, &cap_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(cap == cap_returned);

  // Two attributes were created, a named and an anonymous
  // Anonymous attribute cannot make it to the other side
  // since it gets a default name (constants::default_attr_name)
  // which is prefixed (__attr)
  uint32_t attr_num, attr_num_returned;
  rc = tiledb_array_schema_get_attribute_num(ctx_, array_schema, &attr_num);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_attribute_num(
      ctx_, array_schema_returned, &attr_num_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(attr_num == attr_num_returned);

  // Retrieve domains
  tiledb_domain_t* domain;
  tiledb_array_schema_get_domain(ctx_, array_schema, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_domain_t* domain_returned;
  tiledb_array_schema_get_domain(ctx_, array_schema_returned, &domain_returned);
  REQUIRE(rc == TILEDB_OK);

  // Number of dimensions should be the same
  uint32_t ndim, ndim_returned;
  rc = tiledb_domain_get_ndim(ctx_, domain, &ndim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_get_ndim(ctx_, domain_returned, &ndim_returned);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(ndim == ndim_returned);

  // Check if there is an anonymous dimension
  tiledb_dimension_t* dim;
  rc = tiledb_domain_get_dimension_from_name(ctx_, domain, "", &dim);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_returned;
  rc = tiledb_domain_get_dimension_from_name(
      ctx_, domain_returned, "", &dim_returned);
  REQUIRE(rc == TILEDB_OK);

  tiledb_array_schema_free(&array_schema);
  tiledb_array_schema_free(&array_schema_returned);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArraySchemaCapnp,
    "C API: Test query capnp serialization",
    "[capi], [capnp]") {
  // Create array schema
  tiledb_array_schema_t* array_schema = create_array_schema_simple();

  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string array_name = temp_dir + "capnp_serialization_test";

  // Create array
  int rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Prepare some data for the array
  int data[] = {1, 2, 3, 4};
  uint64_t data_size = sizeof(data);

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Slice only rows 1, 2, 3, 4
  int64_t subarray[] = {1, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", data, &data_size);
  REQUIRE(rc == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_query_t* query_returned;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_returned);
  REQUIRE(rc == TILEDB_OK);
  char* data_serialized;
  uint64_t data_serialized_size = 0;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      &data_serialized,
      &data_serialized_size);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_deserialize(
      ctx_,
      query_returned,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      data_serialized,
      data_serialized_size);
  REQUIRE(rc == TILEDB_OK);

  std::free(data_serialized);

  // Clean up
  tiledb_array_free(&array);
  tiledb_array_schema_free(&array_schema);
  tiledb_query_free(&query);
  tiledb_query_free(&query_returned);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArraySchemaCapnp,
    "C API: Test query capnp serialization performance",
    "[capi], [capnp]") {
  // The array name
  std::string array_name = "/tmp/large_array";

  // Load the array from disc
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Benchmark
  uint64_t subarray[] = {
      500, 997, 1532937600002008000, 1532995200013172000, 1070, 215092501};
  //    2.1M rows, 48MB, 0.0188789 s
  //    uint64_t subarray[] = {1002, 2000, 1532937600009134000,
  //    1532995200013217000, 1114, 215085801};
  //    3.7M rows, 84.8MB, 0.0150247 s
  //    uint64_t subarray[] = {1002, 3000, 1532937600007296000,
  //    1532995200013217000, 1114, 215085801};
  //    7.42M rows, 170MB, 0.0345649 s
  //    uint64_t subarray[] = {1002, 4000, 1532937600005446000,
  //    1532995200013217000, 1114, 215085801};
  //    10.6M rows, 241MB, 0.0261757 s
  //    uint64_t subarray[] = {1500, 7500, 1532937600001903000,
  //    1532995200013217000, 1002, 220035001};
  //    22.1M rows, 505MB, 0.05095 s
  //    uint64_t subarray[] = {1, 8998, 1532937600001903000,
  //    1532995200013244000, 1002, 220038001};
  //    35.5M rows, 812MB, 0.0790097 s

  // From original code
  //    uint64_t subarray[] = {500, 997, 1532937600002008000,
  //    1532995200013172000, 1070, 215092501};
  //    2.1M rows, 48MB, 0.537279 s
  //    uint64_t subarray[] = {1002, 2000, 1532937600009134000,
  //    1532995200013217000, 1114, 215085801};
  //    3.7M rows, 84.8MB, 0.649579 s
  //    uint64_t subarray[] = {1002, 3000, 1532937600007296000,
  //    1532995200013217000, 1114, 215085801};
  //    7.42M rows, 170MB, 1.12898 s
  //    uint64_t subarray[] = {1002, 4000, 1532937600005446000,
  //    1532995200013217000, 1114, 215085801};
  //    10.6M rows, 241MB, 0.899395 s
  //    uint64_t subarray[] = {1500, 7500, 1532937600001903000,
  //    1532995200013217000, 1002, 220035001};
  //    22.1M rows, 505MB, 2.01576 s
  //    uint64_t subarray[] = {1, 8998, 1532937600001903000,
  //    1532995200013244000, 1002, 220038001};
  //    35.5M rows, 812MB, 1.62337 s

  // Initial tests
  //    uint64_t subarray[] = {1, 1, 1532957401004652000, 1532980980383492000,
  //    135901, 114523301};

  // Calculate maximum buffer sizes
  uint64_t coords_size;
  uint64_t data_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "Trade_Volume", subarray, &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  std::vector<uint32_t> data(data_size / sizeof(uint32_t));
  std::vector<uint64_t> coords(coords_size / sizeof(uint64_t));

  // Create the query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, "Trade_Volume", &data[0], &data_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, &coords[0], &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Load array schema from the rest server
  tiledb_query_t* query_returned;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query_returned);
  REQUIRE(rc == TILEDB_OK);

  // Record start time
  auto start = std::chrono::high_resolution_clock::now();

  char* data_serialized;
  uint64_t data_serialized_size = 0;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      &data_serialized,
      &data_serialized_size);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_deserialize(
      ctx_,
      query_returned,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      data_serialized,
      data_serialized_size);
  REQUIRE(rc == TILEDB_OK);

  // Record end time
  auto finish = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> elapsed = finish - start;
  std::cout << "Elapsed time: " << elapsed.count() << " s\n";

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_query_free(&query_returned);
}

#else

TEST_CASE_METHOD(
    ArraySchemaCapnp,
    "C API: Test array schema serialization",
    "[capi], [capnp]") {
  tiledb_array_schema_t* array_schema = create_array_schema();

  tiledb_array_schema_t* array_schema_returned;
  char* data;
  uint64_t data_size = 0;
  int rc = tiledb_array_schema_serialize(
      ctx_,
      array_schema,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      &data,
      &data_size);
  // Eventually should be a runtime error when serialization is disabled in the
  // build.
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_deserialize(
      ctx_,
      &array_schema_returned,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      data,
      data_size);
  REQUIRE(rc == TILEDB_ERR);

  tiledb_array_schema_free(&array_schema);
  std::free(data);
}

#endif  // TILEDB_SERIALIZATION