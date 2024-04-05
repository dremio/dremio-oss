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

package com.dremio.exec.physical.config;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.protostuff.ByteString;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("partition-transform-table")
public class PartitionTransformTableFunctionContext extends TableFunctionContext {
  private final ByteString partitionSpec;
  private final String icebergSchema;

  public PartitionTransformTableFunctionContext(
      @JsonProperty("partitionSpec") ByteString partitionSpec,
      @JsonProperty("icebergSchema") String icebergSchema,
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("columns") List<SchemaPath> columns) {
    super(
        null,
        fullSchema,
        null,
        null,
        null,
        null,
        null,
        null,
        columns,
        null,
        null,
        false,
        false,
        true,
        null);
    this.partitionSpec = partitionSpec;
    this.icebergSchema = icebergSchema;
  }

  public ByteString getPartitionSpec() {
    return partitionSpec;
  }

  public String getIcebergSchema() {
    return icebergSchema;
  }
}
