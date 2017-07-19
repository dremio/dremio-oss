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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.DatasetSplitIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_ID;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_IDENTIFIER;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_ROWS;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_SIZE;
import static com.dremio.service.namespace.DatasetSplitIndexKeys.SPLIT_VERSION;
import static java.lang.String.format;

import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.datastore.SearchTypes.SearchFieldSorting.FieldType;
import com.dremio.datastore.indexed.IndexKey;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionValue;

/**
 * Index dataset splits.
 */
public class DatasetSplitConverter implements DocumentConverter<DatasetSplitId, DatasetSplit> {

  private static final int LARGE_VALUE_CUTOFF = 512;

  public static String buildColumnKey(FieldType fieldType, String columnName) {
    return format("$D$::%s-%s", fieldType.name(), columnName);
  }

  @Override
  public void convert(DocumentWriter writer, DatasetSplitId key, DatasetSplit split) {
    writer.write(SPLIT_ID, key.getSpiltId());
    writer.write(DATASET_ID, key.getDatasetId());
    writer.write(SPLIT_IDENTIFIER, key.getSplitIdentifier());
    writer.write(SPLIT_VERSION, split.getSplitVersion());
    writer.write(SPLIT_ROWS, split.getRowCount());
    writer.write(SPLIT_SIZE, split.getSize());
    if(split.getPartitionValuesList() != null){
      for (PartitionValue pv : split.getPartitionValuesList()) {
        if (pv.getLongValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.LONG, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, Long.class, FieldType.LONG, false, false);
          writer.write(indexKey, pv.getLongValue());
        } else if (pv.getIntValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.INTEGER, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, Integer.class, FieldType.INTEGER, false, false);
          writer.write(indexKey, pv.getIntValue());
        } else if (pv.getBinaryValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.STRING, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, String.class, FieldType.STRING, false, false);
          byte[] bytes = pv.getBinaryValue().toByteArray();
          if(bytes.length < LARGE_VALUE_CUTOFF){
            // don't index large values as they need to be accurate and the store will cut them off
            writer.write(indexKey, bytes);
          }
        } else if (pv.getStringValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.STRING, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, String.class, FieldType.STRING, false, false);
          String str = pv.getStringValue();
          if(str.length() < LARGE_VALUE_CUTOFF){
            // don't index large values as they need to be accurate and the store will cut them off
            writer.write(indexKey, str);
          }
        } else if (pv.getDoubleValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.DOUBLE, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, Double.class, FieldType.DOUBLE, false, false);
          writer.write(indexKey, pv.getDoubleValue());
        } else if (pv.getFloatValue() != null) {
          final String columnKey =  buildColumnKey(FieldType.DOUBLE, pv.getColumn());
          final IndexKey indexKey = new IndexKey(columnKey, columnKey, Double.class, FieldType.DOUBLE, false, false);
          writer.write(indexKey, (double) pv.getFloatValue());
        }
      }
    }
  }
}
