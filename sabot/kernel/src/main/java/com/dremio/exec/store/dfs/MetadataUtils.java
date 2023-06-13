/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.dfs;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.parquet.io.api.Binary;
import org.joda.time.DateTimeConstants;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.connector.metadata.PartitionValue.PartitionValueType;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting.FieldType;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.store.parquet.ParquetFilterCondition.FilterProperties;
import com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils;
import com.dremio.service.namespace.PartitionChunkConverter;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Utils to serialize storage plugin specific structures to/from namespace.
 */
public class MetadataUtils {
  private static final Function<SchemaPath, String> schemaPathStringFunction = new Function<SchemaPath, String>() {
    @Nullable
    @Override
    public String apply(@Nullable SchemaPath input) {
      return input.getAsUnescapedPath();
    }
  };

  public static ScanStats toPojoScanStats(com.dremio.service.namespace.dataset.proto.ScanStats cachedScanStats) {
    if (cachedScanStats.getType() == ScanStatsType.NO_EXACT_ROW_COUNT) {
      return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, cachedScanStats.getRecordCount(), cachedScanStats.getCpuCost(), cachedScanStats.getDiskCost());
    } else if (cachedScanStats.getType() == ScanStatsType.EXACT_ROW_COUNT) {
      return new ScanStats(ScanStats.GroupScanProperty.EXACT_ROW_COUNT, cachedScanStats.getRecordCount(), cachedScanStats.getCpuCost(), cachedScanStats.getDiskCost());
    }
    throw new IllegalArgumentException("Invalid scan stats type " + cachedScanStats.getType());
  }

  public static com.dremio.service.namespace.dataset.proto.ScanStats fromPojoScanStats(ScanStats scanStats) {
    final com.dremio.service.namespace.dataset.proto.ScanStats cachedScanStats = new com.dremio.service.namespace.dataset.proto.ScanStats();

    cachedScanStats.setCpuCost(scanStats.getCpuCost());
    cachedScanStats.setDiskCost(scanStats.getDiskCost());
    cachedScanStats.setRecordCount(scanStats.getRecordCount());

    if (scanStats.getGroupScanProperty().equals(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT)) {
      cachedScanStats.setType(ScanStatsType.NO_EXACT_ROW_COUNT);
    } else if (scanStats.getGroupScanProperty().equals(ScanStats.GroupScanProperty.EXACT_ROW_COUNT)) {
      cachedScanStats.setType(ScanStatsType.EXACT_ROW_COUNT);
    } else {
      throw new IllegalArgumentException("Invalid scan stats type " + scanStats.getGroupScanProperty());
    }
    return cachedScanStats;
  }

  public static List<String> getStringColumnNames(final List<SchemaPath> columns) {
    return Lists.transform(columns, schemaPathStringFunction);
  }

  public static PartitionValue toPartitionValue(final SchemaPath column, final Object value, final MinorType type,
      final PartitionValueType partitionType) {
      final String name = column.getAsUnescapedPath();
    if (value == null) {
      return PartitionValue.of(name, partitionType);
    }

    switch (type) {
      case INT:
      case TIME:
      case INTERVALYEAR:
        return PartitionValue.of(name, ((Number)value).intValue(), partitionType);

      case TIMESTAMP:
        if(value instanceof Binary){
          // int96.
          return PartitionValue.of(name, NanoTimeUtils.getDateTimeValueFromBinary((Binary) value), partitionType);
        }

        return PartitionValue.of(name, ((Number)value).longValue(), partitionType);

      case DATE:
        return PartitionValue.of(name, ((Number)value).longValue() * (long) DateTimeConstants.MILLIS_PER_DAY, partitionType);

      case BIGINT:
      case INTERVALDAY:
        return PartitionValue.of(name, ((Number)value).longValue(), partitionType);

      case VARCHAR:
        if (value instanceof String) { // if the metadata was read from a JSON cache file it maybe a string type
          return PartitionValue.of(name, (String) value, partitionType);
        } else if (value instanceof Binary) {
          return PartitionValue.of(name, new String(((Binary) value).getBytes(), UTF_8), partitionType);
        } else if (value instanceof byte[]) {
          return PartitionValue.of(name, new String((byte[]) value, UTF_8), partitionType);
        } else {
          throw new UnsupportedOperationException("Unable to create column data for type: " + type);
        }

      case VARBINARY:
        if (value instanceof String) { // if the metadata was read from a JSON cache file it maybe a string type
          return PartitionValue.of(name, ByteBuffer.wrap(((String) value).getBytes(UTF_8)), partitionType);
        } else if (value instanceof Binary) {
          return PartitionValue.of(name, ByteBuffer.wrap(((Binary) value).getBytes()), partitionType);
        } else if (value instanceof byte[]) {
          return PartitionValue.of(name, ByteBuffer.wrap((byte[]) value), partitionType);
        } else {
          throw new UnsupportedOperationException("Unable to create column data for type: " + type);
        }

      case FLOAT4:
        return PartitionValue.of(name, ((Number)value).floatValue(), partitionType);

      case FLOAT8:
        return PartitionValue.of(name, ((Number)value).doubleValue(), partitionType);

      case BIT:
        return PartitionValue.of(name, (Boolean) value, partitionType);

      case DECIMAL:
        if (value instanceof Binary) {
          return PartitionValue.of(name, ByteBuffer.wrap(((Binary) value).getBytes()), partitionType);
        } else if (value instanceof byte[]) {
          return PartitionValue.of(name, ByteBuffer.wrap((byte[]) value), partitionType);
        } else if (value instanceof Integer) {
          BigInteger decimal = BigInteger.valueOf(((Number) value).intValue());
          return PartitionValue.of(name, ByteBuffer.wrap(decimal.toByteArray()), partitionType);
        } else if (value instanceof Long) {
          BigInteger decimal = BigInteger.valueOf(((Number) value).longValue());
          return PartitionValue.of(name, ByteBuffer.wrap(decimal.toByteArray()), partitionType);
        }
        return PartitionValue.of(name, partitionType);

      case LIST:
      case UNION:
      case NULL:
      case STRUCT:
      default:
        throw new UnsupportedOperationException(type + " is not supported");
    }
  }

  private static class RangeQueryInput {
    Object min;
    Object max;
    boolean includeMin;
    boolean includeMax;

    public RangeQueryInput(Object value, SqlKind type) {
      this.min = value;
      this.max = value;
      this.includeMin = false;
      this.includeMax = false;
      switch (type) {
        case GREATER_THAN:
          this.max = null;
          break;
        case GREATER_THAN_OR_EQUAL:
          this.max = null;
          this.includeMin = true;
          break;
        case LESS_THAN:
          this.min = null;
          break;
        case LESS_THAN_OR_EQUAL:
          this.min = null;
          this.includeMax = true;
          break;
        case EQUALS:
          this.includeMax = true;
          this.includeMin = true;
          break;
        default:
          throw new UnsupportedOperationException("Invalid kind " + type);
      }
    }
  }

  public static FieldType getFieldType(CompleteType type){
    switch(type.toMinorType()){
    case BIGINT:
    case TIMESTAMP:
    case DATE:
      return FieldType.LONG;

    case INT:
    case TIME:
      return FieldType.INTEGER;

    case FLOAT4:
    case FLOAT8:
      return FieldType.DOUBLE;
    case VARCHAR:
      return FieldType.STRING;
    }

    throw new UnsupportedOperationException(type.toString());
  }

  public static SearchQuery toSplitsSearchQuery(List<FilterProperties> filters, Field field) {
    Preconditions.checkNotNull(field);

    final CompleteType ct = CompleteType.fromField(field);

    final FieldType fieldType = getFieldType(ct);
    final String columnKey = PartitionChunkConverter.buildColumnKey(fieldType, field.getName());
    final List<SearchQuery> filterQueries = Lists.newArrayList();

    for (FilterProperties filter: filters) {
      final RexLiteral literal = filter.getLiteral();
      SearchQuery matchingSplitsQuery = null;
      final RangeQueryInput rangeQueryInput;
      switch (ct.toMinorType()) {
        case BIGINT:

          rangeQueryInput = new RangeQueryInput(
            ((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP).longValue(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeLong(columnKey, (Long) rangeQueryInput.min, (Long) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        case TIME:
          rangeQueryInput = new RangeQueryInput((int) ((GregorianCalendar) literal.getValue()).getTimeInMillis(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeInt(columnKey, (Integer) rangeQueryInput.min, (Integer) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        case VARCHAR:
          if (literal.getValue() instanceof  NlsString) {
            rangeQueryInput = new RangeQueryInput(((NlsString) literal.getValue()).getValue(), filter.getKind());
          } else {
            rangeQueryInput = new RangeQueryInput((literal.getValue3().toString()), filter.getKind());
          }
          matchingSplitsQuery = SearchQueryUtils.newRangeTerm(columnKey, (String) rangeQueryInput.min, (String) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        case FLOAT4:
          rangeQueryInput = new RangeQueryInput(((BigDecimal) literal.getValue()).floatValue(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeFloat(columnKey, (Float) rangeQueryInput.min, (Float) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        case FLOAT8:
          rangeQueryInput = new RangeQueryInput(((BigDecimal) literal.getValue()).doubleValue(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeDouble(columnKey, (Double) rangeQueryInput.min, (Double) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        case INT:
          rangeQueryInput = new RangeQueryInput(((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP).intValue(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeInt(columnKey, (Integer) rangeQueryInput.min, (Integer) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;
        case DATE:
        case TIMESTAMP:
          rangeQueryInput = new RangeQueryInput(((GregorianCalendar) literal.getValue()).getTimeInMillis(), filter.getKind());
          matchingSplitsQuery = SearchQueryUtils.newRangeLong(columnKey, (Long) rangeQueryInput.min, (Long) rangeQueryInput.max, rangeQueryInput.includeMin, rangeQueryInput.includeMax);
          break;

        default:
          throw new UnsupportedOperationException("type not supported " + ct.toMinorType());
      }
      if (matchingSplitsQuery != null) {
        filterQueries.add(matchingSplitsQuery);
      }
    }

    return SearchQueryUtils.and(filterQueries);
  }
}
