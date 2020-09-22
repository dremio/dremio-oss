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
package com.dremio.exec.store;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.tablefunctions.ExternalQuery;
import com.dremio.exec.tablefunctions.ExternalQueryScanPrel;
import com.google.common.base.Preconditions;

/**
 * A sub-interface of StoragePlugin that provides an API for plugins with external query capabilities.
 */
public interface SupportsExternalQuery {
  String EXTERNAL_QUERY = "external_query";

  // Only external query syntax with exactly 2 path elements is supported.
  // E.g. source.external_query is supported.
  // E.g. external_query function call without source name is not supported.
  // E.g. source.schema.external_query is not supported.
  int FUNCTION_CALL_NUM_PATHS = 2;

  /**
   * Gets the physical operator used to implement the external query for this source.
   * @param creator The physical plan creator.
   * @param prel   The Prel representing the external query.
   * @param schema The schema of the external query result.
   * @param sql    The external query SQL text.
   * @return The physical operator used to implement execution of an external query.
   * @throws IOException
   */
  PhysicalOperator getExternalQueryPhysicalOperator(PhysicalPlanCreator creator, ExternalQueryScanPrel prel, BatchSchema schema,
                                                    String sql) throws IOException;

  /**
   * Checks whether provided tableSchemaPath contains an external query table function call. Returns
   * an instance of external query function if the plugin supports it.
   *
   * @param schemaBuilder Function used to construct a schema for the external query the user specified.
   * @param rowTypeBuilder Function used to construct row type for the external query RelNodes, from a BatchSchema.
   * @param pluginId  The pluginId from where this external query will query against.
   * @param tableSchemaPath The table schema path of the function call.
   * @return an external query function. Returns an empty list if external query function is not
   * found in the tableSchemaPath, or the plugin does not support external query table function.
   */
  static Optional<org.apache.calcite.schema.Function> getExternalQueryFunction(Function<String, BatchSchema> schemaBuilder,
                                                                               Function<BatchSchema, RelDataType> rowTypeBuilder,
                                                                               StoragePluginId pluginId, List<String> tableSchemaPath) {
    Preconditions.checkNotNull(schemaBuilder, "schemaBuilder cannot be null.");
    Preconditions.checkNotNull(rowTypeBuilder, "rowTypeBuilder cannot be null.");
    return (tableSchemaPath.size() == FUNCTION_CALL_NUM_PATHS &&
      tableSchemaPath.get(tableSchemaPath.size() - 1).equalsIgnoreCase(EXTERNAL_QUERY))?
      Optional.of(new ExternalQuery(schemaBuilder, rowTypeBuilder, pluginId)) : Optional.empty();
  }
}
