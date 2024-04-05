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

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.ops.DremioCatalogReader;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

public class RelOptTableImplSerializer extends Serializer<DremioPrepareTable> {

  private final DremioCatalogReader catalog;

  public RelOptTableImplSerializer(final DremioCatalogReader catalog) {
    this.catalog = Preconditions.checkNotNull(catalog, "catalog is required");
  }

  @Override
  public void write(final Kryo kryo, final Output output, final DremioPrepareTable relOptTable) {
    final List<String> path = relOptTable.getQualifiedName();
    kryo.writeObject(output, path);
  }

  @Override
  public DremioPrepareTable read(
      final Kryo kryo, final Input input, final Class<DremioPrepareTable> type) {
    final List<String> path = kryo.readObject(input, ArrayList.class);
    return catalog.getTable(path);
  }
}
