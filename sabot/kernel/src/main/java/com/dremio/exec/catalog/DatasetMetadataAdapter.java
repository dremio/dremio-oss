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
package com.dremio.exec.catalog;

import java.util.Collections;
import java.util.List;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.ByteString;

/**
 * Adapts a {@link DatasetConfig} to behave like {@link DatasetMetadata}.
 */
@VisibleForTesting
public class DatasetMetadataAdapter implements DatasetMetadata {

  private final DatasetConfig datasetConfig;

  private final BatchSchema recordSchema;
  private final DatasetStats datasetStats;

  @VisibleForTesting
  public DatasetMetadataAdapter(DatasetConfig datasetConfig) {
    this.datasetConfig = datasetConfig;

    this.recordSchema = BatchSchema.deserialize(datasetConfig.getRecordSchema());
    this.datasetStats = new DatasetStats() {
      @Override
      public long getRecordCount() {
        return datasetConfig.getReadDefinition().getScanStats().getRecordCount();
      }

      @Override
      public double getScanFactor() {
        return datasetConfig.getReadDefinition().getScanStats().getScanFactor();
      }
    };
  }

  @Override
  public DatasetStats getDatasetStats() {
    return datasetStats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return recordSchema;
  }

  @Override
  public List<String> getPartitionColumns() {
    return getOrEmpty(datasetConfig.getReadDefinition().getPartitionColumnsList());
  }

  @Override
  public List<String> getSortColumns() {
    return getOrEmpty(datasetConfig.getReadDefinition().getSortColumnsList());
  }

  private static List<String> getOrEmpty(List<String> list) {
    return list == null ? Collections.emptyList() : Collections.unmodifiableList(list);
  }

  @Override
  public BytesOutput getExtraInfo() {
    return os -> ByteString.writeTo(os, datasetConfig.getReadDefinition().getExtendedProperty());
  }

}
