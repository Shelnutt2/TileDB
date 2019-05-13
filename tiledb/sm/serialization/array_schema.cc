/**
 * @file   array_schema.cc
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
 * This file defines serialization functions for ArraySchema.
 */

#include "capnp/compat/json.h"
#include "capnp/serialize.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/serialization/utils.h"

namespace tiledb {
namespace rest {
namespace capnp {

tiledb::sm::Status filter_pipeline_to_capnp(
    const tiledb::sm::FilterPipeline* filter_pipeline,
    FilterPipeline::Builder* filter_pipeline_builder) {
  STATS_FUNC_IN(serialization_filter_pipeline_to_capnp);

  if (filter_pipeline == nullptr)
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing filter pipeline; filter pipeline is null."));

  const unsigned num_filters = filter_pipeline->size();
  if (num_filters == 0)
    return tiledb::sm::Status::Ok();

  auto filter_list_builder = filter_pipeline_builder->initFilters(num_filters);
  for (unsigned i = 0; i < num_filters; i++) {
    const auto* filter = filter_pipeline->get_filter(i);
    auto filter_builder = filter_list_builder[i];
    filter_builder.setType(filter_type_str(filter->type()));

    switch (filter->type()) {
      case tiledb::sm::FilterType::FILTER_BIT_WIDTH_REDUCTION: {
        uint32_t window;
        RETURN_NOT_OK(filter->get_option(
            tiledb::sm::FilterOption::BIT_WIDTH_MAX_WINDOW, &window));
        auto data = filter_builder.initData();
        data.setUint32(window);
        break;
      }
      case tiledb::sm::FilterType::FILTER_POSITIVE_DELTA: {
        uint32_t window;
        RETURN_NOT_OK(filter->get_option(
            tiledb::sm::FilterOption::POSITIVE_DELTA_MAX_WINDOW, &window));
        auto data = filter_builder.initData();
        data.setUint32(window);
        break;
      }
      case tiledb::sm::FilterType::FILTER_GZIP:
      case tiledb::sm::FilterType::FILTER_ZSTD:
      case tiledb::sm::FilterType::FILTER_LZ4:
      case tiledb::sm::FilterType::FILTER_RLE:
      case tiledb::sm::FilterType::FILTER_BZIP2:
      case tiledb::sm::FilterType::FILTER_DOUBLE_DELTA: {
        int32_t level;
        RETURN_NOT_OK(filter->get_option(
            tiledb::sm::FilterOption::COMPRESSION_LEVEL, &level));
        auto data = filter_builder.initData();
        data.setInt32(level);
        break;
      }
      default:
        break;
    }
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_filter_pipeline_to_capnp);
}

tiledb::sm::Status filter_pipeline_from_capnp(
    const FilterPipeline::Reader& filter_pipeline_reader,
    std::unique_ptr<tiledb::sm::FilterPipeline>* filter_pipeline) {
  STATS_FUNC_IN(serialization_filter_pipeline_from_capnp);

  filter_pipeline->reset(new tiledb::sm::FilterPipeline);
  if (!filter_pipeline_reader.hasFilters())
    return tiledb::sm::Status::Ok();

  auto filter_list_reader = filter_pipeline_reader.getFilters();
  for (const auto& filter_reader : filter_list_reader) {
    tiledb::sm::FilterType type = tiledb::sm::FilterType::FILTER_NONE;
    RETURN_NOT_OK(filter_type_enum(filter_reader.getType().cStr(), &type));
    std::unique_ptr<tiledb::sm::Filter> filter(
        tiledb::sm::Filter::create(type));
    if (filter == nullptr)
      return LOG_STATUS(tiledb::sm::Status::RestError(
          "Error deserializing filter pipeline; failed to create filter."));

    switch (filter->type()) {
      case tiledb::sm::FilterType::FILTER_BIT_WIDTH_REDUCTION: {
        auto data = filter_reader.getData();
        uint32_t window = data.getUint32();
        RETURN_NOT_OK(filter->set_option(
            tiledb::sm::FilterOption::BIT_WIDTH_MAX_WINDOW, &window));
        break;
      }
      case tiledb::sm::FilterType::FILTER_POSITIVE_DELTA: {
        auto data = filter_reader.getData();
        uint32_t window = data.getUint32();
        RETURN_NOT_OK(filter->set_option(
            tiledb::sm::FilterOption::POSITIVE_DELTA_MAX_WINDOW, &window));
        break;
      }
      case tiledb::sm::FilterType::FILTER_GZIP:
      case tiledb::sm::FilterType::FILTER_ZSTD:
      case tiledb::sm::FilterType::FILTER_LZ4:
      case tiledb::sm::FilterType::FILTER_RLE:
      case tiledb::sm::FilterType::FILTER_BZIP2:
      case tiledb::sm::FilterType::FILTER_DOUBLE_DELTA: {
        auto data = filter_reader.getData();
        int32_t level = data.getInt32();
        RETURN_NOT_OK(filter->set_option(
            tiledb::sm::FilterOption::COMPRESSION_LEVEL, &level));
        break;
      }
      default:
        break;
    }

    RETURN_NOT_OK((*filter_pipeline)->add_filter(*filter));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_filter_pipeline_from_capnp);
}

tiledb::sm::Status attribute_to_capnp(
    const tiledb::sm::Attribute* attribute,
    Attribute::Builder* attribute_builder) {
  STATS_FUNC_IN(serialization_attribute_to_capnp);

  if (attribute == nullptr)
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing attribute; attribute is null."));

  attribute_builder->setName(attribute->name());
  attribute_builder->setType(tiledb::sm::datatype_str(attribute->type()));
  attribute_builder->setCellValNum(attribute->cell_val_num());

  const auto* filters = attribute->filters();
  auto filter_pipeline_builder = attribute_builder->initFilterPipeline();
  RETURN_NOT_OK(filter_pipeline_to_capnp(filters, &filter_pipeline_builder));

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_attribute_to_capnp);
}

tiledb::sm::Status attribute_from_capnp(
    const Attribute::Reader& attribute_reader,
    std::unique_ptr<tiledb::sm::Attribute>* attribute) {
  STATS_FUNC_IN(serialization_attribute_from_capnp);

  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  RETURN_NOT_OK(
      tiledb::sm::datatype_enum(attribute_reader.getType(), &datatype));

  attribute->reset(
      new tiledb::sm::Attribute(attribute_reader.getName(), datatype));
  RETURN_NOT_OK(
      (*attribute)->set_cell_val_num(attribute_reader.getCellValNum()));

  // Set filter pipelines
  if (attribute_reader.hasFilterPipeline()) {
    auto filter_pipeline_reader = attribute_reader.getFilterPipeline();
    std::unique_ptr<tiledb::sm::FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(filter_pipeline_reader, &filters));
    RETURN_NOT_OK((*attribute)->set_filter_pipeline(filters.get()));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_attribute_from_capnp);
}

tiledb::sm::Status dimension_to_capnp(
    const tiledb::sm::Dimension* dimension,
    Dimension::Builder* dimension_builder) {
  STATS_FUNC_IN(serialization_dimension_to_capnp);

  if (dimension == nullptr)
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing dimension; dimension is null."));

  dimension_builder->setName(dimension->name());
  dimension_builder->setType(tiledb::sm::datatype_str(dimension->type()));
  dimension_builder->setNullTileExtent(dimension->tile_extent() == nullptr);

  auto domain_builder = dimension_builder->initDomain();
  RETURN_NOT_OK(utils::set_capnp_array_ptr(
      domain_builder, dimension->type(), dimension->domain(), 2));

  if (dimension->tile_extent() != nullptr) {
    auto tile_extent_builder = dimension_builder->initTileExtent();
    RETURN_NOT_OK(utils::set_capnp_scalar(
        tile_extent_builder, dimension->type(), dimension->tile_extent()));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_dimension_to_capnp);
}

tiledb::sm::Status dimension_from_capnp(
    const Dimension::Reader& dimension_reader,
    std::unique_ptr<tiledb::sm::Dimension>* dimension) {
  STATS_FUNC_IN(serialization_dimension_from_capnp);

  tiledb::sm::Datatype dim_type = tiledb::sm::Datatype::ANY;
  RETURN_NOT_OK(
      tiledb::sm::datatype_enum(dimension_reader.getType().cStr(), &dim_type));
  dimension->reset(
      new tiledb::sm::Dimension(dimension_reader.getName(), dim_type));

  auto domain_reader = dimension_reader.getDomain();
  tiledb::sm::Buffer domain_buffer;
  RETURN_NOT_OK(
      utils::copy_capnp_list(domain_reader, dim_type, &domain_buffer));
  RETURN_NOT_OK((*dimension)->set_domain(domain_buffer.data()));

  if (!dimension_reader.getNullTileExtent()) {
    auto tile_extent_reader = dimension_reader.getTileExtent();
    switch (dim_type) {
      case tiledb::sm::Datatype::INT8: {
        auto val = tile_extent_reader.getInt8();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::UINT8: {
        auto val = tile_extent_reader.getUint8();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        auto val = tile_extent_reader.getInt16();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::UINT16: {
        auto val = tile_extent_reader.getUint16();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::INT32: {
        auto val = tile_extent_reader.getInt32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::UINT32: {
        auto val = tile_extent_reader.getUint32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        auto val = tile_extent_reader.getInt64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        auto val = tile_extent_reader.getUint64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        auto val = tile_extent_reader.getFloat32();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        auto val = tile_extent_reader.getFloat64();
        RETURN_NOT_OK((*dimension)->set_tile_extent(&val));
        break;
      }
      default:
        return LOG_STATUS(tiledb::sm::Status::Error(
            "Error deserializing dimension; unknown datatype."));
    }
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_dimension_from_capnp);
}

tiledb::sm::Status domain_to_capnp(
    const tiledb::sm::Domain* domain, Domain::Builder* domainBuilder) {
  STATS_FUNC_IN(serialization_domain_to_capnp);

  if (domain == nullptr)
    return LOG_STATUS(
        tiledb::sm::Status::Error("Error serializing domain; domain is null."));

  domainBuilder->setType(tiledb::sm::datatype_str(domain->type()));
  domainBuilder->setTileOrder(tiledb::sm::layout_str(domain->tile_order()));
  domainBuilder->setCellOrder(tiledb::sm::layout_str(domain->cell_order()));

  const unsigned ndims = domain->dim_num();
  auto dimensions_builder = domainBuilder->initDimensions(ndims);
  for (unsigned i = 0; i < ndims; i++) {
    auto dim_builder = dimensions_builder[i];
    RETURN_NOT_OK(dimension_to_capnp(domain->dimension(i), &dim_builder));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_domain_to_capnp);
}

tiledb::sm::Status domain_from_capnp(
    const Domain::Reader& domain_reader,
    std::unique_ptr<tiledb::sm::Domain>* domain) {
  STATS_FUNC_IN(serialization_domain_from_capnp);

  tiledb::sm::Datatype datatype = tiledb::sm::Datatype::ANY;
  RETURN_NOT_OK(tiledb::sm::datatype_enum(domain_reader.getType(), &datatype));
  domain->reset(new tiledb::sm::Domain(datatype));

  auto dimensions = domain_reader.getDimensions();
  for (const auto& dimension : dimensions) {
    std::unique_ptr<tiledb::sm::Dimension> dim;
    RETURN_NOT_OK(dimension_from_capnp(dimension, &dim));
    RETURN_NOT_OK((*domain)->add_dimension(dim.get()));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_domain_from_capnp);
}

tiledb::sm::Status array_schema_to_capnp(
    const tiledb::sm::ArraySchema* array_schema,
    ArraySchema::Builder* array_schema_builder) {
  STATS_FUNC_IN(serialization_array_schema_to_capnp);

  if (array_schema == nullptr)
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing array schema; array schema is null."));

  array_schema_builder->setUri(array_schema->array_uri().to_string());
  array_schema_builder->setVersion(
      kj::arrayPtr(tiledb::sm::constants::library_version, 3));
  array_schema_builder->setArrayType(
      tiledb::sm::array_type_str(array_schema->array_type()));
  array_schema_builder->setTileOrder(
      tiledb::sm::layout_str(array_schema->tile_order()));
  array_schema_builder->setCellOrder(
      tiledb::sm::layout_str(array_schema->cell_order()));
  array_schema_builder->setCapacity(array_schema->capacity());

  // Set coordinate filters
  const tiledb::sm::FilterPipeline* coords_filters =
      array_schema->coords_filters();
  FilterPipeline::Builder coords_filters_builder =
      array_schema_builder->initCoordsFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(coords_filters, &coords_filters_builder));

  // Set offset filters
  const tiledb::sm::FilterPipeline* offsets_filters =
      array_schema->cell_var_offsets_filters();
  FilterPipeline::Builder offsets_filters_builder =
      array_schema_builder->initOffsetFilterPipeline();
  RETURN_NOT_OK(
      filter_pipeline_to_capnp(offsets_filters, &offsets_filters_builder));

  // Domain
  auto domain_builder = array_schema_builder->initDomain();
  RETURN_NOT_OK(domain_to_capnp(array_schema->domain(), &domain_builder));

  // Attributes
  const unsigned num_attrs = array_schema->attribute_num();
  auto attributes_buidler = array_schema_builder->initAttributes(num_attrs);
  for (size_t i = 0; i < num_attrs; i++) {
    auto attribute_builder = attributes_buidler[i];
    RETURN_NOT_OK(
        attribute_to_capnp(array_schema->attribute(i), &attribute_builder));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_to_capnp);
}

tiledb::sm::Status array_schema_from_capnp(
    const ArraySchema::Reader& schema_reader,
    std::unique_ptr<tiledb::sm::ArraySchema>* array_schema) {
  STATS_FUNC_IN(serialization_array_schema_from_capnp);

  tiledb::sm::ArrayType array_type = tiledb::sm::ArrayType::DENSE;
  RETURN_NOT_OK(
      tiledb::sm::array_type_enum(schema_reader.getArrayType(), &array_type));
  array_schema->reset(new tiledb::sm::ArraySchema(array_type));

  tiledb::sm::Layout layout = tiledb::sm::Layout::ROW_MAJOR;
  RETURN_NOT_OK(
      tiledb::sm::layout_enum(schema_reader.getTileOrder().cStr(), &layout));
  (*array_schema)->set_tile_order(layout);
  RETURN_NOT_OK(
      tiledb::sm::layout_enum(schema_reader.getCellOrder().cStr(), &layout));

  (*array_schema)
      ->set_array_uri(tiledb::sm::URI(schema_reader.getUri().cStr()));
  (*array_schema)->set_cell_order(layout);
  (*array_schema)->set_capacity(schema_reader.getCapacity());

  auto domain_reader = schema_reader.getDomain();
  std::unique_ptr<tiledb::sm::Domain> domain;
  RETURN_NOT_OK(domain_from_capnp(domain_reader, &domain));
  RETURN_NOT_OK((*array_schema)->set_domain(domain.get()));

  // Set coords filter pipelines
  if (schema_reader.hasCoordsFilterPipeline()) {
    auto reader = schema_reader.getCoordsFilterPipeline();
    std::unique_ptr<tiledb::sm::FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(reader, &filters));
    RETURN_NOT_OK((*array_schema)->set_coords_filter_pipeline(filters.get()));
  }

  // Set offsets filter pipelines
  if (schema_reader.hasOffsetFilterPipeline()) {
    auto reader = schema_reader.getOffsetFilterPipeline();
    std::unique_ptr<tiledb::sm::FilterPipeline> filters;
    RETURN_NOT_OK(filter_pipeline_from_capnp(reader, &filters));
    RETURN_NOT_OK(
        (*array_schema)->set_cell_var_offsets_filter_pipeline(filters.get()));
  }

  // Set attributes
  auto attributes_reader = schema_reader.getAttributes();
  for (const auto& attr_reader : attributes_reader) {
    std::unique_ptr<tiledb::sm::Attribute> attribute;
    RETURN_NOT_OK(attribute_from_capnp(attr_reader, &attribute));
    RETURN_NOT_OK((*array_schema)->add_attribute(attribute.get(), false));
  }

  // Initialize
  RETURN_NOT_OK((*array_schema)->init());

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_from_capnp);
}

tiledb::sm::Status array_schema_serialize(
    tiledb::sm::ArraySchema* array_schema,
    tiledb::sm::SerializationType serialize_type,
    tiledb::sm::Buffer* serialized_buffer) {
  STATS_FUNC_IN(serialization_array_schema_serialize);

  try {
    ::capnp::MallocMessageBuilder message;
    ArraySchema::Builder arraySchemaBuilder = message.initRoot<ArraySchema>();
    RETURN_NOT_OK(array_schema_to_capnp(array_schema, &arraySchemaBuilder));

    serialized_buffer->reset_size();
    serialized_buffer->reset_offset();

    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        ::capnp::JsonCodec json;
        kj::String capnp_json = json.encode(arraySchemaBuilder);
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
        const auto nbytes = message_chars.size();
        RETURN_NOT_OK(serialized_buffer->realloc(nbytes));
        RETURN_NOT_OK(serialized_buffer->write(message_chars.begin(), nbytes));
        break;
      }
      default: {
        return LOG_STATUS(tiledb::sm::Status::Error(
            "Error serializing array schema; Unknown serialization type "
            "passed"));
      }
    }

  } catch (kj::Exception& e) {
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error serializing array schema; exception " + std::string(e.what())));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_serialize);
}

tiledb::sm::Status array_schema_deserialize(
    tiledb::sm::ArraySchema** array_schema,
    tiledb::sm::SerializationType serialize_type,
    const tiledb::sm::Buffer& serialized_buffer) {
  STATS_FUNC_IN(serialization_array_schema_deserialize);

  try {
    std::unique_ptr<tiledb::sm::ArraySchema> decoded_array_schema = nullptr;

    switch (serialize_type) {
      case tiledb::sm::SerializationType::JSON: {
        ::capnp::JsonCodec json;
        ::capnp::MallocMessageBuilder message_builder;
        rest::capnp::ArraySchema::Builder array_schema_builder =
            message_builder.initRoot<rest::capnp::ArraySchema>();
        json.decode(
            kj::StringPtr(static_cast<const char*>(serialized_buffer.data())),
            array_schema_builder);
        rest::capnp::ArraySchema::Reader array_schema_reader =
            array_schema_builder.asReader();
        RETURN_NOT_OK(rest::capnp::array_schema_from_capnp(
            array_schema_reader, &decoded_array_schema));
        break;
      }
      case tiledb::sm::SerializationType::CAPNP: {
        const auto mBytes =
            reinterpret_cast<const kj::byte*>(serialized_buffer.data());
        ::capnp::FlatArrayMessageReader reader(kj::arrayPtr(
            reinterpret_cast<const ::capnp::word*>(mBytes),
            serialized_buffer.size() / sizeof(::capnp::word)));
        ArraySchema::Reader array_schema_reader =
            reader.getRoot<rest::capnp::ArraySchema>();
        RETURN_NOT_OK(rest::capnp::array_schema_from_capnp(
            array_schema_reader, &decoded_array_schema));
        break;
      }
      default: {
        return LOG_STATUS(tiledb::sm::Status::Error(
            "Error deserializing array schema; Unknown serialization type "
            "passed"));
      }
    }

    if (decoded_array_schema == nullptr)
      return LOG_STATUS(tiledb::sm::Status::Error(
          "Error serializing array schema; deserialized schema is null"));

    *array_schema = decoded_array_schema.release();
  } catch (kj::Exception& e) {
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error deserializing array schema; kj::Exception: " +
        std::string(e.getDescription().cStr())));
  } catch (std::exception& e) {
    return LOG_STATUS(tiledb::sm::Status::Error(
        "Error deserializing array schema; exception " +
        std::string(e.what())));
  }

  return tiledb::sm::Status::Ok();

  STATS_FUNC_OUT(serialization_array_schema_deserialize);
}

}  // namespace capnp
}  // namespace rest
}  // namespace tiledb
