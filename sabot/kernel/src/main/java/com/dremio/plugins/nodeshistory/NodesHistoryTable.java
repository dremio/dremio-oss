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
package com.dremio.plugins.nodeshistory;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public enum NodesHistoryTable {
  METRICS(
      "metrics",
      ImmutableList.of(
          // Fields are marked nullable unless the implementation of NodeMetricsService ensures they
          // cannot be null (e.g. nullable if derived from optional proto fields used from
          // NodeEndpoint)
          Field.notNullable("timestamp", Types.MinorType.TIMESTAMPMILLI.getType()),
          Field.nullable("name", Types.MinorType.VARCHAR.getType()),
          Field.nullable("hostname", Types.MinorType.VARCHAR.getType()),
          Field.nullable("ip", Types.MinorType.VARCHAR.getType()),
          Field.nullable("cpu", Types.MinorType.FLOAT4.getType()),
          Field.nullable("memory", Types.MinorType.FLOAT4.getType()),
          Field.notNullable("status", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("is_master", Types.MinorType.BIT.getType()),
          Field.notNullable("is_coordinator", Types.MinorType.BIT.getType()),
          Field.notNullable("is_executor", Types.MinorType.BIT.getType()),
          Field.nullable("start", Types.MinorType.TIMESTAMPMILLI.getType())));

  private final String name;
  private final List<Field> schema;

  NodesHistoryTable(String name, List<Field> schema) {
    this.name = name;
    this.schema = schema;
  }

  public String getName() {
    return name;
  }

  public List<Field> getSchema() {
    return schema;
  }

  public List<String> getPath() {
    return List.of(NodesHistoryStoreConfig.STORAGE_PLUGIN_NAME, name);
  }
}
