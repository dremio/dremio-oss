/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.logical.serialization.serializers;


import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.RelOptTableImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;

public class RelOptTableImplSerializer extends Serializer<RelOptTableImpl> {

  private final CalciteCatalogReader catalog;

  public RelOptTableImplSerializer(final CalciteCatalogReader catalog) {
    this.catalog = Preconditions.checkNotNull(catalog, "catalog is required");
  }

  @Override
  public void write(final Kryo kryo, final Output output, final RelOptTableImpl relOptTable) {
    final List<String> path = relOptTable.getQualifiedName();
    kryo.writeObject(output, path);
  }

  @Override
  public RelOptTableImpl read(final Kryo kryo, final Input input, final Class<RelOptTableImpl> type) {
    final List<String> path = kryo.readObject(input, ArrayList.class);
    final RelOptTableImpl relOptTable = catalog.getTable(path);
    return relOptTable;
  }
}
