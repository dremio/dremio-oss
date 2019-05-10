/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.stream.Stream;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;
import com.dremio.exec.record.BatchSchema;

public class CurrentSchemaOption implements GetMetadataOption, GetDatasetOption, ListPartitionChunkOption {

  private final BatchSchema batchSchema;

  public CurrentSchemaOption(BatchSchema batchSchema) {
    this.batchSchema = batchSchema;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  public static BatchSchema getSchema(MetadataOption... options) {
    return Stream.of(options).filter(o -> o instanceof CurrentSchemaOption).findFirst().map(o -> ((CurrentSchemaOption) o).getBatchSchema()).orElse(null);
  }

}
