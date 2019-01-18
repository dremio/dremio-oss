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
package com.dremio.exec.planner.serialization;

import org.apache.calcite.plan.RelOptCluster;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.serialization.kryo.KryoRelSerializerFactory;

/**
 * Abstract class that defines how to get plan serializers and deserializers.
 */
public abstract class RelSerializerFactory {

  private static final String PATH = "dremio.planning.serializer";
  public final static RelSerializerFactory DEFAULT = new KryoRelSerializerFactory(null);

  /**
   * Get a serializer for the given cluster.
   * @param cluster The cluster to serialize from.
   * @return
   */
  public abstract LogicalPlanSerializer getSerializer(RelOptCluster cluster);

  /**
   * Get a deserializer for the given cluster and catalog.
   * @param cluster Cluster to read into.
   * @param catalog Catalog to use for deserializing tables.
   * @param registry FunctionImplementationRegistry to use for deserializing Dremio operators.
   * @return The plan deserializer.
   */
  public abstract LogicalPlanDeserializer getDeserializer(
      final RelOptCluster cluster,
      final DremioCatalogReader catalog,
      final FunctionImplementationRegistry registry);

  public static RelSerializerFactory getFactory(SabotConfig config, ScanResult scanResult) {
    if(config.hasPath(PATH)) {
      return config.getInstance(PATH, RelSerializerFactory.class, scanResult);
    }
    return DEFAULT;
  }
}
