/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.math.BigDecimal;
import java.nio.charset.Charset;
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
import com.dremio.common.types.MinorType;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting.FieldType;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.store.parquet.FilterCondition.FilterProperties;
import com.dremio.exec.store.parquet.ParquetReaderUtility.NanoTimeUtils;
import com.dremio.service.namespace.DatasetSplitConverter;
import com.dremio.service.namespace.dataset.proto.PartitionValue;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * Utils to serialize storage plugin specific structures to/from namespace.
 */
public class MetadataUtils {
  private static final Charset UTF8 = Charset.forName("UTF-8");

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

  public static PartitionValue toPartitionValue(final SchemaPath column, final Object value, final MinorType type) {
    final PartitionValue partitionValue = new PartitionValue();
    partitionValue.setColumn(column.getAsUnescapedPath());
    switch (type) {
      case INT:
      case TIME:
      case INTERVALYEAR:
        partitionValue.setIntValue(value == null? null : ((Number)value).intValue());
        break;
      case TIMESTAMP:
        if (value == null) {
          partitionValue.setLongValue(null);
          break;
        }
        if(value instanceof Binary){
          // int96.
          partitionValue.setLongValue(NanoTimeUtils.getDateTimeValueFromBinary((Binary) value));
          break;
        }
        partitionValue.setLongValue(value == null? null : ((Number)value).longValue());
        break;

      case DATE:
        partitionValue.setLongValue(value == null? null : ((Number)value).intValue() * (long) DateTimeConstants.MILLIS_PER_DAY);
        break;

      case BIGINT:
      case INTERVALDAY:
        partitionValue.setLongValue(value == null? null : ((Number)value).longValue());
        break;

      case VARCHAR:
        if (value == null) {
          partitionValue.setStringValue(null);
        } else {
          if (value instanceof String) { // if the metadata was read from a JSON cache file it maybe a string type
            partitionValue.setStringValue((String) value);
          } else if (value instanceof Binary) {
            partitionValue.setStringValue(new String(((Binary) value).getBytes(), UTF8));
          } else if (value instanceof byte[]) {
            partitionValue.setStringValue(new String((byte[]) value, UTF8));
          } else {
            throw new UnsupportedOperationException("Unable to create column data for type: " + type);
          }
        }
        break;

      case VARBINARY:
        if (value == null) {
          partitionValue.setStringValue(null);
        } else {
          if (value instanceof String) { // if the metadata was read from a JSON cache file it maybe a string type
            partitionValue.setBinaryValue(ByteString.copyFrom(((String) value).getBytes(UTF8)));
          } else if (value instanceof Binary) {
            partitionValue.setBinaryValue(ByteString.copyFrom(((Binary) value).getBytes()));
          } else if (value instanceof byte[]) {
            partitionValue.setBinaryValue(ByteString.copyFrom((byte[]) value));
          } else {
            throw new UnsupportedOperationException("Unable to create column data for type: " + type);
          }
        }
        break;

      case FLOAT4:
        partitionValue.setFloatValue(value == null? null : ((Number)value).floatValue());
        break;

      case FLOAT8:
        partitionValue.setDoubleValue(value == null? null : ((Number)value).doubleValue());
        break;

      case BIT:
        partitionValue.setBitValue(value == null? null : (Boolean) value);
        break;

      case DECIMAL:
      case LIST:
      case UNION:
      case NULL:
      case MAP:
        throw new UnsupportedOperationException(type + " is not supported");
    }
    return partitionValue;
  }

  private static class RangeQueryInput {
    Object min;
    Object max;
    boolean includeMin;
    boolean includeMax;

    public RangeQueryInput(Object value, SqlKind type) {
      this.min = this.max = value;
      this.includeMax = this.includeMin = false;
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
          this.includeMax = this.includeMin = true;
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
      return FieldType.LONG;

    case INT:
    case DATE:
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
    final String columnKey = DatasetSplitConverter.buildColumnKey(fieldType, field.getName());
    final SearchQuery partitionColumnNotDefinedQuery = SearchQueryUtils.newDoesNotExistQuery(columnKey);
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

        case DATE:
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

    return SearchQueryUtils.or(SearchQueryUtils.and(filterQueries), partitionColumnNotDefinedQuery);
  }
}
