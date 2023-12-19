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
package com.dremio.exec.planner.serialization.kryo.serializers;


import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.service.namespace.NamespaceKey;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

public class RelOptNamespaceTableSerializer extends Serializer<RelOptNamespaceTable> {

  private final DremioCatalogReader catalog;
  private final RelOptCluster cluster;

  public RelOptNamespaceTableSerializer(final DremioCatalogReader catalog, final RelOptCluster cluster) {
    this.catalog = Preconditions.checkNotNull(catalog, "catalog is required");
    this.cluster = cluster;
  }

  @Override
  public void write(final Kryo kryo, final Output output, final RelOptNamespaceTable relOptTable) {
    final List<String> path = relOptTable.getQualifiedName();
    kryo.writeObject(output, path);
  }

  @Override
  public RelOptNamespaceTable read(final Kryo kryo, final Input input, final Class<RelOptNamespaceTable> type) {
    final List<String> path = kryo.readObject(input, ArrayList.class);
    final DremioPrepareTable relOptTable = catalog.getTable(path);
    if(relOptTable == null){
      throw new IllegalStateException("Unable to retrieve table: " + new NamespaceKey(path));
    }
    NamespaceTable namespace = relOptTable.unwrap(NamespaceTable.class);
    return new RelOptNamespaceTable(namespace, cluster);
  }
}
