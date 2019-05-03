/**
 * @file   unit-capi-serialized_queries.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB Inc.
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
 * Tests for query serialization/deserialization.
 */

#include "catch.hpp"

#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

namespace {

struct SerializationFx {
  const std::string tmpdir = "serialization_test_dir";
  const std::string array_name = "testarray";
  const std::string array_uri = tmpdir + "/" + array_name;

  Context ctx;
  VFS vfs;

  SerializationFx()
      : vfs(ctx) {
    if (vfs.is_dir(tmpdir))
      vfs.remove_dir(tmpdir);
    vfs.create_dir(tmpdir);
  }

  ~SerializationFx() {
    if (vfs.is_dir(tmpdir))
      vfs.remove_dir(tmpdir);
  }

  void create_array(tiledb_array_type_t type) {
    ArraySchema schema(ctx, type);
    Domain domain(ctx);
    domain.add_dimension(Dimension::create<int32_t>(ctx, "d1", {1, 10}, 2))
        .add_dimension(Dimension::create<int32_t>(ctx, "d2", {1, 10}, 2));
    schema.set_domain(domain);

    schema.add_attribute(Attribute::create<uint32_t>(ctx, "a1"))
        .add_attribute(Attribute::create<std::array<uint32_t, 2>>(ctx, "a2"))
        .add_attribute(Attribute::create<std::vector<char>>(ctx, "a3"));

    Array::create(array_uri, schema);
  }

  void write_dense_array() {
    std::vector<int32_t> subarray = {1, 10, 1, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells =
        (subarray[1] - subarray[0] + 1) * (subarray[3] - subarray[2] + 1);
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);
    Serialization::deserialize_query(ctx, serialized, &query);
  }

  void write_sparse_array() {
    std::vector<int32_t> coords = {1, 1, 2, 2, 3, 3, 4, 4, 5,  5,
                                   6, 6, 7, 7, 8, 8, 9, 9, 10, 10};
    std::vector<uint32_t> a1;
    std::vector<uint32_t> a2;
    std::vector<char> a3_data;
    std::vector<uint64_t> a3_offsets;

    const unsigned ncells = 10;
    for (unsigned i = 0; i < ncells; i++) {
      a1.push_back(i);
      a2.push_back(i);
      a2.push_back(2 * i);

      std::string a3 = "a";
      for (unsigned j = 0; j < i; j++)
        a3.push_back('a');
      a3_offsets.push_back(a3_data.size());
      a3_data.insert(a3_data.end(), a3.begin(), a3.end());
    }

    Array array(ctx, array_uri, TILEDB_WRITE);
    Query query(ctx, array);
    query.set_layout(TILEDB_UNORDERED);
    query.set_coordinates(coords);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_WRITE);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);
    Serialization::deserialize_query(ctx, serialized, &query);
  }
};

}  // namespace

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, dense",
    "[query], [dense], [serialization]") {
  create_array(TILEDB_DENSE);
  write_dense_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);

    // Deserialize into new third query (with orig buffers set).
    Query query3(ctx, array);
    query3.set_buffer("a1", a1);
    query3.set_buffer("a2", a2);
    query3.set_buffer("a3", a3_offsets, a3_data);
    Serialization::deserialize_query(ctx, serialized, &query3);
    REQUIRE(query3.query_status() == Query::Status::COMPLETE);

    auto result_el = query3.result_buffer_elements();
    REQUIRE(result_el["a1"].second == 100);
    REQUIRE(result_el["a2"].second == 200);
    REQUIRE(result_el["a3"].first == 100);
    REQUIRE(result_el["a3"].second == 5050);
  }

  SECTION("- Read subarray") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {3, 4, 3, 4};

    query.set_subarray(subarray);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);

    // Deserialize into new third query (with orig buffers set).
    Query query3(ctx, array);
    query3.set_buffer("a1", a1);
    query3.set_buffer("a2", a2);
    query3.set_buffer("a3", a3_offsets, a3_data);
    Serialization::deserialize_query(ctx, serialized, &query3);
    REQUIRE(query3.query_status() == Query::Status::COMPLETE);

    auto result_el = query3.result_buffer_elements();
    REQUIRE(result_el["a1"].second == 4);
    REQUIRE(result_el["a2"].second == 8);
    REQUIRE(result_el["a3"].first == 4);
    REQUIRE(result_el["a3"].second == 114);
  }

  SECTION("- Incomplete read") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<uint32_t> a1(4);
    std::vector<uint32_t> a2(4);
    std::vector<char> a3_data(60);
    std::vector<uint64_t> a3_offsets(4);
    std::vector<int32_t> subarray = {3, 4, 3, 4};
    query.set_subarray(subarray);

    auto set_buffers = [&](Query& q) {
      q.set_buffer("a1", a1);
      q.set_buffer("a2", a2);
      q.set_buffer("a3", a3_offsets, a3_data);
    };

    auto serialize_and_submit = [&](Query& q) {
      std::vector<uint8_t> serialized;
      Serialization::serialize_query(ctx, q, &serialized);
      Array array2(ctx, array_uri, TILEDB_READ);
      Query new_query(ctx, array2);
      Serialization::deserialize_query(ctx, serialized, &new_query);
      new_query.submit();
      Serialization::serialize_query(ctx, new_query, &serialized);
      Serialization::deserialize_query(ctx, serialized, &q);
    };

    // Serialize into a copy and submit.
    set_buffers(query);
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);

    // Deserialize into new third query (with orig buffers set).
    Query query3(ctx, array);
    set_buffers(query3);
    Serialization::deserialize_query(ctx, serialized, &query3);
    REQUIRE(query3.query_status() == Query::Status::INCOMPLETE);

    auto result_el = query3.result_buffer_elements();
    REQUIRE(result_el["a1"].second == 2);
    REQUIRE(result_el["a2"].second == 4);
    REQUIRE(result_el["a3"].first == 2);
    REQUIRE(result_el["a3"].second == 47);

    // Reset buffers, serialize and resubmit
    set_buffers(query3);
    serialize_and_submit(query3);

    REQUIRE(query3.query_status() == Query::Status::INCOMPLETE);
    result_el = query3.result_buffer_elements();
    REQUIRE(result_el["a1"].second == 1);
    REQUIRE(result_el["a2"].second == 2);
    REQUIRE(result_el["a3"].first == 1);
    REQUIRE(result_el["a3"].second == 33);

    // Reset buffers, serialize and resubmit
    set_buffers(query3);
    serialize_and_submit(query3);

    REQUIRE(query3.query_status() == Query::Status::COMPLETE);
    result_el = query3.result_buffer_elements();
    REQUIRE(result_el["a1"].second == 1);
    REQUIRE(result_el["a2"].second == 2);
    REQUIRE(result_el["a3"].first == 1);
    REQUIRE(result_el["a3"].second == 34);
  }
}

TEST_CASE_METHOD(
    SerializationFx,
    "Query serialization, sparse",
    "[query], [sparse], [serialization]") {
  create_array(TILEDB_SPARSE);
  write_sparse_array();

  SECTION("- Read all") {
    Array array(ctx, array_uri, TILEDB_READ);
    Query query(ctx, array);
    std::vector<int32_t> coords(1000);
    std::vector<uint32_t> a1(1000);
    std::vector<uint32_t> a2(1000);
    std::vector<char> a3_data(1000 * 100);
    std::vector<uint64_t> a3_offsets(1000);
    std::vector<int32_t> subarray = {1, 10, 1, 10};

    query.set_subarray(subarray);
    query.set_coordinates(coords);
    query.set_buffer("a1", a1);
    query.set_buffer("a2", a2);
    query.set_buffer("a3", a3_offsets, a3_data);

    // Serialize into a copy and submit.
    std::vector<uint8_t> serialized;
    Serialization::serialize_query(ctx, query, &serialized);
    Array array2(ctx, array_uri, TILEDB_READ);
    Query query2(ctx, array2);
    Serialization::deserialize_query(ctx, serialized, &query2);
    query2.submit();
    Serialization::serialize_query(ctx, query2, &serialized);

    // Deserialize into new third query (with orig buffers set).
    Query query3(ctx, array);
    query3.set_coordinates(coords);
    query3.set_buffer("a1", a1);
    query3.set_buffer("a2", a2);
    query3.set_buffer("a3", a3_offsets, a3_data);
    Serialization::deserialize_query(ctx, serialized, &query3);
    REQUIRE(query3.query_status() == Query::Status::COMPLETE);

    auto result_el = query3.result_buffer_elements();
    REQUIRE(result_el[TILEDB_COORDS].second == 20);
    REQUIRE(result_el["a1"].second == 10);
    REQUIRE(result_el["a2"].second == 20);
    REQUIRE(result_el["a3"].first == 10);
    REQUIRE(result_el["a3"].second == 55);
  }
}