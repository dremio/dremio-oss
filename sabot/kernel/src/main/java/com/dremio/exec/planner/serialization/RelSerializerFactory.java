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
package com.dremio.exec.planner.serialization;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.planner.serialization.kryo.KryoRelSerializerFactory;
import com.dremio.exec.store.CatalogService;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.sql.SqlOperatorTable;

/** Abstract class that defines how to get plan serializers and deserializers. */
public abstract class RelSerializerFactory {

  private static final String PLANNING_PATH = "dremio.planning.serializer";
  private static final String LEGACY_PLANNING_PATH = "dremio.planning.legacy.serializer";
  private static final String PROFILE_PATH = "dremio.profile.serializer";
  public static RelSerializerFactory DEFAULT = new KryoRelSerializerFactory(null);

  /**
   * Get a serializer for the given cluster.
   *
   * @param cluster The cluster to serialize from.
   * @return
   */
  public abstract LogicalPlanSerializer getSerializer(
      RelOptCluster cluster, SqlOperatorTable sqlOperatorTable);

  /**
   * Get a deserializer for the given cluster and catalog.
   *
   * @param cluster Cluster to read into.
   * @param catalog Catalog to use for deserializing tables.
   * @param sqlOperatorTable SqlOperatorTable to use for deserializing Dremio operators.
   * @return The plan deserializer.
   */
  public abstract LogicalPlanDeserializer getDeserializer(
      final RelOptCluster cluster,
      final DremioCatalogReader catalog,
      final SqlOperatorTable sqlOperatorTable,
      final CatalogService catalogService);

  public LogicalPlanDeserializer getDeserializer(
      final RelOptCluster cluster,
      final DremioCatalogReader catalog,
      final SqlOperatorTable sqlOperatorTable) {
    return getDeserializer(cluster, catalog, sqlOperatorTable, null);
  }

  public static RelSerializerFactory getPlanningFactory(SabotConfig config, ScanResult scanResult) {
    return getFactory(config, scanResult, PLANNING_PATH);
  }

  public static RelSerializerFactory getLegacyPlanningFactory(
      SabotConfig config, ScanResult scanResult) {
    return getFactory(config, scanResult, LEGACY_PLANNING_PATH);
  }

  public static RelSerializerFactory getProfileFactory(SabotConfig config, ScanResult scanResult) {
    return getFactory(config, scanResult, PROFILE_PATH);
  }

  private static RelSerializerFactory getFactory(
      SabotConfig config, ScanResult scanResult, String property) {
    if (config.hasPath(property)) {
      return config.getInstance(property, RelSerializerFactory.class, scanResult);
    }
    return DEFAULT;
  }
}
