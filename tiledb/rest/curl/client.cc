/**
 * @file   client.cc
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
 * This file implements curl client helper functions.
 */

#include "tiledb/rest/curl/client.h"
#include "capnp/compat/json.h"
#include "tiledb/rest/capnp/array.h"
#include "tiledb/rest/capnp/query.h"
#include "tiledb/rest/capnp/tiledb-rest.capnp.h"
#include "tiledb/rest/curl/curl.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace rest {

/**
 * Calls curl_easy_init(), wrapping the returned object in a safe pointer
 * that will be cleaned up automatically.
 */
std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> init_curl_safe() {
  return std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>(
      curl_easy_init(), curl_easy_cleanup);
}

/**
 * Gets relevant REST config params from the given config object.
 *
 * @param config TileDB config instance
 * @param organization Set to `rest.organization`
 * @param rest_server Set to `rest.server_address`
 * @param serialization_type Set to `rest.server_serialization_format`
 * @return Status
 */
tiledb::sm::Status get_rest_info_from_config(
    const tiledb::sm::Config& config,
    std::string* organization,
    std::string* rest_server,
    tiledb::sm::SerializationType* serialization_type) {
  const char* c_str;

  RETURN_NOT_OK(config.get("rest.organization", &c_str));
  if (c_str == nullptr)
    return LOG_STATUS(
        tiledb::sm::Status::RestError("REST API error; `rest.organization` "
                                      "config parameter cannot be null."));
  *organization = std::string(c_str);

  RETURN_NOT_OK(config.get("rest.server_address", &c_str));
  if (c_str != nullptr)
    *rest_server = std::string(c_str);

  RETURN_NOT_OK(config.get("rest.server_serialization_format", &c_str));
  if (c_str != nullptr)
    RETURN_NOT_OK(
        tiledb::sm::serialization_type_enum(c_str, serialization_type));

  return tiledb::sm::Status::Ok();
}

tiledb::sm::Status get_array_schema_from_rest(
    const tiledb::sm::Config& config,
    const std::string& uri,
    tiledb::sm::ArraySchema** array_schema) {
  STATS_FUNC_IN(serialization_get_array_schema_from_rest);

  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(uri);

  // Get the data
  tiledb::sm::Buffer returned_data;
  RETURN_NOT_OK(curlc.get_data(url, serialization_type, &returned_data));
  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error getting array schema from REST; server returned no data."));

  return rest::capnp::array_schema_deserialize(
      array_schema, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_get_array_schema_from_rest);
}

tiledb::sm::Status post_array_schema_to_rest(
    const tiledb::sm::Config& config,
    const std::string& uri,
    tiledb::sm::ArraySchema* array_schema) {
  STATS_FUNC_IN(serialization_post_array_schema_to_rest);

  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(rest::capnp::array_schema_serialize(
      array_schema, serialization_type, &serialized));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(uri);

  tiledb::sm::Buffer returned_data;
  return curlc.post_data(url, serialization_type, &serialized, &returned_data);

  STATS_FUNC_OUT(serialization_post_array_schema_to_rest);
}

tiledb::sm::Status deregister_array_from_rest(
    const tiledb::sm::Config& config, const std::string& uri) {
  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(uri) + "/deregister";

  tiledb::sm::Buffer returned_data;
  return curlc.delete_data(url, serialization_type, &returned_data);
}

tiledb::sm::Status get_array_non_empty_domain(
    const tiledb::sm::Config& config,
    tiledb::sm::Array* array,
    void* domain,
    bool* is_empty) {
  STATS_FUNC_IN(serialization_get_array_non_empty_domain);

  if (array == nullptr)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot get array non-empty domain; array is null"));
  if (array->array_uri().to_string().empty())
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Cannot get array non-empty domain; array URI is empty"));

  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(array->array_uri().to_string()) +
                    "/non_empty_domain";

  // Get the data
  tiledb::sm::Buffer returned_data;
  RETURN_NOT_OK(
      curlc.get_data(url, tiledb::sm::SerializationType::JSON, &returned_data));

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(
        tiledb::sm::Status::RestError("Error getting array non-empty domain "
                                      "from REST; server returned no data."));

  // Currently only json data is supported, so let's decode it here.
  ::capnp::JsonCodec json;
  ::capnp::MallocMessageBuilder message_builder;
  rest::capnp::NonEmptyDomain::Builder nonEmptyDomainBuilder =
      message_builder.initRoot<rest::capnp::NonEmptyDomain>();
  json.decode(
      kj::StringPtr(static_cast<const char*>(returned_data.data())),
      nonEmptyDomainBuilder);
  rest::capnp::NonEmptyDomain::Reader nonEmptyDomainReader =
      nonEmptyDomainBuilder.asReader();
  *is_empty = nonEmptyDomainReader.getIsEmpty();

  tiledb::sm::Datatype domainType = array->array_schema()->domain()->type();

  // If there is a nonEmptyDomain we need to set domain variables
  if (nonEmptyDomainReader.hasNonEmptyDomain()) {
    rest::capnp::Map<::capnp::Text, rest::capnp::DomainArray>::Reader
        nonEmptyDomainList = nonEmptyDomainReader.getNonEmptyDomain();

    size_t domainPosition = 0;
    // Loop through dimension's domain in order
    for (auto entry : nonEmptyDomainList.getEntries()) {
      rest::capnp::DomainArray::Reader domainArrayReader = entry.getValue();
      switch (domainType) {
        case tiledb::sm::Datatype::INT8: {
          if (domainArrayReader.hasInt8()) {
            ::capnp::List<int8_t>::Reader domainList =
                domainArrayReader.getInt8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT8: {
          if (domainArrayReader.hasUint8()) {
            ::capnp::List<uint8_t>::Reader domainList =
                domainArrayReader.getUint8();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint8_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          if (domainArrayReader.hasInt16()) {
            ::capnp::List<int16_t>::Reader domainList =
                domainArrayReader.getInt16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT16: {
          if (domainArrayReader.hasUint16()) {
            ::capnp::List<uint16_t>::Reader domainList =
                domainArrayReader.getUint16();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint16_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          if (domainArrayReader.hasInt32()) {
            ::capnp::List<int32_t>::Reader domainList =
                domainArrayReader.getInt32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT32: {
          if (domainArrayReader.hasUint32()) {
            ::capnp::List<uint32_t>::Reader domainList =
                domainArrayReader.getUint32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint32_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          if (domainArrayReader.hasInt64()) {
            ::capnp::List<int64_t>::Reader domainList =
                domainArrayReader.getInt64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<int64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          if (domainArrayReader.hasUint64()) {
            ::capnp::List<uint64_t>::Reader domainList =
                domainArrayReader.getUint64();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<uint64_t*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          if (domainArrayReader.hasFloat32()) {
            ::capnp::List<float>::Reader domainList =
                domainArrayReader.getFloat32();
            for (size_t i = 0; i < domainList.size(); i++, domainPosition++) {
              static_cast<float*>(domain)[domainPosition] = domainList[i];
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
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
          return tiledb::sm::Status::Error(
              "unknown domain type in trying to get non_empty_domain from "
              "rest");
      }
    }
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_get_array_non_empty_domain);
}

tiledb::sm::Status submit_query_to_rest(
    const tiledb::sm::Config& config,
    const std::string& uri,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_submit_query_to_rest);

  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  // Serialize data to send
  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(rest::capnp::query_serialize(
      true, query, serialization_type, &serialized));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(uri) + "/query/submit?type=" +
                    tiledb::sm::query_type_str(query->type());

  tiledb::sm::Buffer returned_data;
  auto st =
      curlc.post_data(url, serialization_type, &serialized, &returned_data);

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error submitting query to REST; server returned no data."));

  // Deserialize data returned
  return rest::capnp::query_deserialize(
      true, query, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_submit_query_to_rest);
}

tiledb::sm::Status finalize_query_to_rest(
    const tiledb::sm::Config& config,
    const std::string& uri,
    tiledb::sm::Query* query) {
  STATS_FUNC_IN(serialization_finalize_query_to_rest);

  std::string organization, rest_server;
  tiledb::sm::SerializationType serialization_type;
  RETURN_NOT_OK(get_rest_info_from_config(
      config, &organization, &rest_server, &serialization_type));

  // Serialize data to send
  tiledb::sm::Buffer serialized;
  RETURN_NOT_OK(rest::capnp::query_serialize(
      true, query, serialization_type, &serialized));

  // Init curl and form the URL
  Curl curlc(config);
  RETURN_NOT_OK(curlc.init());
  std::string url = std::string(rest_server) + "/v1/arrays/" + organization +
                    "/" + curlc.url_escape(uri) + "/query/finalize?type=" +
                    tiledb::sm::query_type_str(query->type());

  tiledb::sm::Buffer returned_data;
  auto st =
      curlc.post_data(url, serialization_type, &serialized, &returned_data);

  if (returned_data.data() == nullptr || returned_data.size() == 0)
    return LOG_STATUS(tiledb::sm::Status::RestError(
        "Error finalizing query; server returned no data."));

  // Deserialize data returned
  return rest::capnp::query_deserialize(
      true, query, serialization_type, returned_data);

  STATS_FUNC_OUT(serialization_finalize_query_to_rest);
}

}  // namespace rest
}  // namespace tiledb