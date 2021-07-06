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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.util.List;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;

/**
 * Implementation of MetadataProvider for Hive plugins
 */
public class HiveMetadataProvider extends MetadataProvider {

  private final DatasetMetadata datasetMetadata;

  public HiveMetadataProvider(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, DatasetMetadata datasetMetadata) {
    super(config, sqlRefreshDataset);
    this.datasetMetadata = datasetMetadata;
  }

  public BatchSchema getTableSchema() {
    return new BatchSchema(datasetMetadata.getRecordSchema().getFields());
  }

  public List<String> getPartitionColumn() {
    return datasetMetadata.getPartitionColumns();
  }
}
