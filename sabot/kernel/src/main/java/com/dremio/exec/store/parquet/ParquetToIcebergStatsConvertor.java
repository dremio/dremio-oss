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
package com.dremio.exec.store.parquet;

import static org.apache.iceberg.types.Conversions.toByteBuffer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;

import com.dremio.exec.ExecConstants;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * Class to support stats conversions from Parquet types to Iceberg types.
 */
public class ParquetToIcebergStatsConvertor {
  public static Metrics toMetrics(OperatorContext context, ParquetMetadata metadata, Schema fileSchema) {
    long rowCount = 0;
    Map<Integer, Long> columnSizes = new HashMap<>();
    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    Map<Integer, Literal<?>> lowerBounds = new HashMap<>();
    Map<Integer, Literal<?>> upperBounds = new HashMap<>();
    Set<Integer> missingStats = new HashSet<>();

    List<BlockMetaData> blocks = metadata.getBlocks();
    for (BlockMetaData block : blocks) {
      rowCount += block.getRowCount();
      for (ColumnChunkMetaData column : block.getColumns()) {
        ColumnPath columnPath = column.getPath();
        if (isNestedType(columnPath)) {
          // Do not store stats for nested types as queries on them are not common
          continue;
        }

        Types.NestedField field = fileSchema.findField(columnPath.toDotString());
        int fieldId = field.fieldId();
        columnSizes.merge(fieldId, column.getTotalSize(), (x, y) -> y + column.getTotalSize());
        valueCounts.merge(fieldId, column.getValueCount(), (x, y) -> y + column.getValueCount());

        Statistics stats = column.getStatistics();
        if (stats == null) {
          missingStats.add(fieldId);
        } else if (!stats.isEmpty()) {
          nullValueCounts.merge(fieldId, stats.getNumNulls(), (x, y) -> y + stats.getNumNulls());
          boolean icebergMinMaxEnabled = context.getOptions().getOption(ExecConstants.ENABLE_ICEBERG_MIN_MAX);
          if (icebergMinMaxEnabled && stats.hasNonNullValue()) {
            updateMin(lowerBounds, fieldId, toLiteral(field.type(), column.getPrimitiveType(), stats.genericGetMin()));
            updateMax(upperBounds, fieldId, toLiteral(field.type(), column.getPrimitiveType(), stats.genericGetMax()));
          }
        }
      }
    }

    // discard accumulated values if any stats were missing
    for (Integer fieldId : missingStats) {
      nullValueCounts.remove(fieldId);
      lowerBounds.remove(fieldId);
      upperBounds.remove(fieldId);
    }

    return new Metrics(rowCount, columnSizes, valueCounts, nullValueCounts,
      toBufferMap(fileSchema, lowerBounds), toBufferMap(fileSchema, upperBounds));
  }

  private static boolean isNestedType(ColumnPath columnPath) {
    return columnPath.size() != 1;
  }

  private static Literal<Object> toLiteral(Type type, PrimitiveType primitiveType, Comparable comparable) {
    return ParquetToIcebergLiteralConvertor.fromParquetPrimitive(type, primitiveType, comparable);
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMin(Map<Integer, Literal<?>> lowerBounds, int id, Literal<T> min) {
    Literal<T> currentMin = (Literal<T>) lowerBounds.get(id);
    if (currentMin == null || min.comparator().compare(min.value(), currentMin.value()) < 0) {
      lowerBounds.put(id, min);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> void updateMax(Map<Integer, Literal<?>> upperBounds, int id, Literal<T> max) {
    Literal<T> currentMax = (Literal<T>) upperBounds.get(id);
    if (currentMax == null || max.comparator().compare(max.value(), currentMax.value()) > 0) {
      upperBounds.put(id, max);
    }
  }

  private static Map<Integer, ByteBuffer> toBufferMap(Schema schema, Map<Integer, Literal<?>> map) {
    Map<Integer, ByteBuffer> bufferMap = new HashMap<>();
    for (Map.Entry<Integer, Literal<?>> entry : map.entrySet()) {
      bufferMap.put(entry.getKey(), toByteBuffer(schema.findType(entry.getKey()).typeId(), entry.getValue().value()));
    }
    return bufferMap;
  }
}
