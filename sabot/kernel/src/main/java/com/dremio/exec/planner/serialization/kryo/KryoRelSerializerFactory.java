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
package com.dremio.exec.planner.serialization.kryo;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.serialization.LogicalPlanDeserializer;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.store.CatalogService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.sql.SqlOperatorTable;

/** RelSerializerFactory that uses Kryo. */
public class KryoRelSerializerFactory extends RelSerializerFactory {

  public KryoRelSerializerFactory(ScanResult scanResult) {}

  @Override
  public LogicalPlanSerializer getSerializer(
      RelOptCluster cluster, SqlOperatorTable sqlOperatorTable) {
    return KryoLogicalPlanSerializers.forSerialization(cluster);
  }

  @Override
  public LogicalPlanDeserializer getDeserializer(
      RelOptCluster cluster,
      DremioCatalogReader catalog,
      SqlOperatorTable sqlOperatorTable,
      CatalogService catalogService) {
    return getDeserializer(cluster, catalog, sqlOperatorTable);
  }

  @Override
  public LogicalPlanDeserializer getDeserializer(
      RelOptCluster cluster, DremioCatalogReader catalog, SqlOperatorTable sqlOperatorTable) {
    return KryoLogicalPlanSerializers.forDeserialization(cluster, catalog);
  }
}
