/**
 * @file   rest_client.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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
 * This file declares a REST client class.
 */

#include <capnp/compat/json.h>

#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"

namespace tiledb {
namespace sm {

Status RestClient::init(const Config* config) {
  if (config == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing rest client; config is null."));

  config_ = config;

  const char* c_str;
  RETURN_NOT_OK(config_->get("rest.organization", &c_str));
  if (c_str == nullptr)
    return LOG_STATUS(
        Status::RestError("Error initializing rest client; `rest.organization` "
                          "config parameter cannot be null."));
  organization_ = std::string(c_str);

  RETURN_NOT_OK(config_->get("rest.server_address", &c_str));
  if (c_str != nullptr)
    rest_server_ = std::string(c_str);

  RETURN_NOT_OK(config_->get("rest.server_serialization_format", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(serialization_type_enum(c_str, &serialization_type_));

  return Status::Ok();
}

Status RestClient::get_array_schema_from_rest(
    const std::string& uri, ArraySchema** array_schema) {
  STATS_FUNC_IN(serialization_get_array_schema_from_rest);

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(uri);

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(url, serialization_type_, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error getting array schema from REST; server returned no data."));

  return serialization::array_schema_deserialize(
      array_schema, serialization_type_, returned_data);

  STATS_FUNC_OUT(serialization_get_array_schema_from_rest);
}

Status RestClient::post_array_schema_to_rest(
    const std::string& uri, ArraySchema* array_schema) {
  STATS_FUNC_IN(serialization_post_array_schema_to_rest);

  Buffer serialized;
  RETURN_NOT_OK(serialization::array_schema_serialize(
      array_schema, serialization_type_, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(uri);

  Buffer returned_data;
  return curlc.post_data(url, serialization_type_, &serialized, &returned_data);

  STATS_FUNC_OUT(serialization_post_array_schema_to_rest);
}

Status RestClient::deregister_array_from_rest(const std::string& uri) {
  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(uri) + "/deregister";

  Buffer returned_data;
  return curlc.delete_data(url, serialization_type_, &returned_data);
}

Status RestClient::get_array_non_empty_domain(
    Array* array, void* domain, bool* is_empty) {
  STATS_FUNC_IN(serialization_get_array_non_empty_domain);

  if (array == nullptr)
    return LOG_STATUS(
        Status::RestError("Cannot get array non-empty domain; array is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(Status::RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(array->array_uri().to_string()) +
                    "/non_empty_domain";

  // Get the data
  Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(url, SerializationType::JSON, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error getting array non-empty domain "
                          "from REST; server returned no data."));

  // Currently only json data is supported, so let's decode it here.
  ::capnp::JsonCodec json;
  ::capnp::MallocMessageBuilder message_builder;
  serialization::capnp::NonEmptyDomain::Builder nonEmptyDomainBuilder =
      message_builder.initRoot<serialization::capnp::NonEmptyDomain>();
  json.decode(
      kj::StringPtr(static_cast<const char*>(returned_data.data())),
      nonEmptyDomainBuilder);
  serialization::capnp::NonEmptyDomain::Reader nonEmptyDomainReader =
      nonEmptyDomainBuilder.asReader();
  *is_empty = nonEmptyDomainReader.getIsEmpty();

  Datatype domainType = array->array_schema()->domain()->type();

  // If there is a nonEmptyDomain we need to set domain variables
  if (nonEmptyDomainReader.hasNonEmptyDomain()) {
    serialization::capnp::Map<
        ::capnp::Text,
        serialization::capnp::DomainArray>::Reader nonEmptyDomainList =
        nonEmptyDomainReader.getNonEmptyDomain();

    size_t domainPosition = 0;
    // Loop through dimension's domain in order
    for (auto entry : nonEmptyDomainList.getEntries()) {
      serialization::capnp::DomainArray::Reader domainArrayReader =
          entry.getValue();
      switch (domainType) {
        case Datatype::INT8: {
          if (domainArrayReader.hasInt8()) {
            ::capnp::List<int8_t>::Reader domainList =
                domainArrayReader.getInt8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::UINT8: {
          if (domainArrayReader.hasUint8()) {
            ::capnp::List<uint8_t>::Reader domainList =
                domainArrayReader.getUint8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::INT16: {
          if (domainArrayReader.hasInt16()) {
            ::capnp::List<int16_t>::Reader domainList =
                domainArrayReader.getInt16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::UINT16: {
          if (domainArrayReader.hasUint16()) {
            ::capnp::List<uint16_t>::Reader domainList =
                domainArrayReader.getUint16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::INT32: {
          if (domainArrayReader.hasInt32()) {
            ::capnp::List<int32_t>::Reader domainList =
                domainArrayReader.getInt32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::UINT32: {
          if (domainArrayReader.hasUint32()) {
            ::capnp::List<uint32_t>::Reader domainList =
                domainArrayReader.getUint32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::INT64: {
          if (domainArrayReader.hasInt64()) {
            ::capnp::List<int64_t>::Reader domainList =
                domainArrayReader.getInt64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::UINT64: {
          if (domainArrayReader.hasUint64()) {
            ::capnp::List<uint64_t>::Reader domainList =
                domainArrayReader.getUint64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::FLOAT32: {
          if (domainArrayReader.hasFloat32()) {
            ::capnp::List<float>::Reader domainList =
                domainArrayReader.getFloat32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<float*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case Datatype::FLOAT64: {
          if (domainArrayReader.hasFloat64()) {
            ::capnp::List<double>::Reader domainList =
                domainArrayReader.getFloat64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<double*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        default:
          return Status::Error(
              "unknown domain type in trying to get non_empty_domain from "
              "rest");
      }
    }
  }

  return Status::Ok();

  STATS_FUNC_OUT(serialization_get_array_non_empty_domain);
}

Status RestClient::submit_query_to_rest(const std::string& uri, Query* query) {
  STATS_FUNC_IN(serialization_submit_query_to_rest);

  // Serialize data to send
  Buffer serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(uri) +
                    "/query/submit?type=" + query_type_str(query->type());

  Buffer returned_data;
  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(Status::RestError(
        "Error submitting query to REST; server returned no data."));

  // Deserialize data returned
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, query);

  STATS_FUNC_OUT(serialization_submit_query_to_rest);
}

Status RestClient::finalize_query_to_rest(
    const std::string& uri, Query* query) {
  STATS_FUNC_IN(serialization_finalize_query_to_rest);

  // Serialize data to send
  Buffer serialized;
  RETURN_NOT_OK(serialization::query_serialize(
      query, serialization_type_, true, &serialized));

  // Init curl and form the URL
  Curl curlc;
  RETURN_NOT_OK(curlc.init(config_));
  std::string url = rest_server_ + "/v1/arrays/" + organization_ + "/" +
                    curlc.url_escape(uri) +
                    "/query/finalize?type=" + query_type_str(query->type());

  Buffer returned_data;
  RETURN_NOT_OK(
      curlc.post_data(url, serialization_type_, &serialized, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        Status::RestError("Error finalizing query; server returned no data."));

  // Deserialize data returned
  return serialization::query_deserialize(
      returned_data, serialization_type_, true, query);

  STATS_FUNC_OUT(serialization_finalize_query_to_rest);
}

}  // namespace sm
}  // namespace tiledb