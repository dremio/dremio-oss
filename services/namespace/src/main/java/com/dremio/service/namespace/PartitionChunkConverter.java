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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.DatasetSplitIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_IDENTIFIER;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_ROWS;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_SIZE;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_VERSION;
import static java.lang.String.format;

import com.dremio.datastore.SearchTypes.SearchFieldSorting.FieldType;
import com.dremio.datastore.api.DocumentConverter;
import com.dremio.datastore.api.DocumentWriter;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionValue;

/** Index dataset splits. */
public class PartitionChunkConverter
    implements DocumentConverter<PartitionChunkId, PartitionChunk> {
  private Integer version = 0;

  @Override
  public Integer getVersion() {
    return version;
  }

  private static final int LARGE_VALUE_CUTOFF = 512;

  public static String buildColumnKey(FieldType fieldType, String columnName) {
    return format("$D$::%s-%s", fieldType.name(), columnName);
  }

  @Override
  public void convert(DocumentWriter writer, PartitionChunkId key, PartitionChunk split) {
    writer.write(SPLIT_ID, key.getSplitId());
    writer.write(DATASET_ID, key.getDatasetId());
    writer.write(SPLIT_IDENTIFIER, key.getSplitIdentifier());
    writer.write(SPLIT_VERSION, key.getSplitVersion());
    writer.write(SPLIT_ROWS, split.getRowCount());
    writer.write(SPLIT_SIZE, split.getSize());
    if (split.getPartitionValuesList() != null) {
      for (PartitionValue pv : split.getPartitionValuesList()) {
        if (pv.hasLongValue()) {
          final String columnKey = buildColumnKey(FieldType.LONG, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, Long.class)
                  .setSortedValueType(FieldType.LONG)
                  .build();
          writer.write(indexKey, pv.getLongValue());
        } else if (pv.hasIntValue()) {
          final String columnKey = buildColumnKey(FieldType.INTEGER, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, Integer.class)
                  .setSortedValueType(FieldType.INTEGER)
                  .build();
          writer.write(indexKey, pv.getIntValue());
        } else if (pv.hasBinaryValue()) {
          final String columnKey = buildColumnKey(FieldType.STRING, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, String.class)
                  .setSortedValueType(FieldType.STRING)
                  .build();
          byte[] bytes = pv.getBinaryValue().toByteArray();
          if (bytes.length < LARGE_VALUE_CUTOFF) {
            // don't index large values as they need to be accurate and the store will cut them off
            writer.write(indexKey, bytes);
          }
        } else if (pv.hasStringValue()) {
          final String columnKey = buildColumnKey(FieldType.STRING, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, String.class)
                  .setSortedValueType(FieldType.STRING)
                  .build();
          String str = pv.getStringValue();
          if (str.length() < LARGE_VALUE_CUTOFF) {
            // don't index large values as they need to be accurate and the store will cut them off
            writer.write(indexKey, str);
          }
        } else if (pv.hasDoubleValue()) {
          final String columnKey = buildColumnKey(FieldType.DOUBLE, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, Double.class)
                  .setSortedValueType(FieldType.DOUBLE)
                  .build();
          writer.write(indexKey, pv.getDoubleValue());
        } else if (pv.hasFloatValue()) {
          final String columnKey = buildColumnKey(FieldType.DOUBLE, pv.getColumn());
          final IndexKey indexKey =
              IndexKey.newBuilder(columnKey, columnKey, Double.class)
                  .setSortedValueType(FieldType.DOUBLE)
                  .build();
          writer.write(indexKey, (double) pv.getFloatValue());
        }
      }
    }
  }
}
