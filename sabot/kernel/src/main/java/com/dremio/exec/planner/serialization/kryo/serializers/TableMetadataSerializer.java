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
package com.dremio.exec.planner.serialization.kryo.serializers;


import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

public class TableMetadataSerializer extends Serializer<TableMetadata> {

  private final DremioCatalogReader catalog;

  public TableMetadataSerializer(final DremioCatalogReader catalog) {
    this.catalog = Preconditions.checkNotNull(catalog, "catalog is required");
  }

  @Override
  public void write(final Kryo kryo, final Output output, final TableMetadata table) {
    try{
      Preconditions.checkArgument(!table.isPruned(), "Cannot serialize a pruned table.");
    }catch(NamespaceException ex){
      throw Throwables.propagate(ex);
    }

    kryo.writeObject(output, table.getName().getPathComponents());
  }

  @Override
  public TableMetadata read(final Kryo kryo, final Input input, final Class<TableMetadata> type) {
    final List<String> path = kryo.readObject(input, ArrayList.class);
    final DremioPrepareTable relOptTable = catalog.getTable(path);
    NamespaceTable namespace = relOptTable.unwrap(NamespaceTable.class);
    return namespace.getDataset();
  }
}
