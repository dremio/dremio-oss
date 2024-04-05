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
package com.dremio.exec.catalog;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

public class DremioSchema extends CalciteSchema {

  private final PlannerCatalog catalog;
  private final NamespaceKey namespaceKey;

  public DremioSchema(PlannerCatalog catalog, NamespaceKey namespaceKey) {
    super(null, null, namespaceKey.getSchemaPath(), null, null, null, null, null, null, null, null);
    this.catalog = catalog;
    this.namespaceKey = namespaceKey;
  }

  @Override
  protected CalciteSchema getImplicitSubSchema(String s, boolean b) {
    NamespaceKey newNamespaceKey = namespaceKey.getChild(s);
    if (catalog.containerExists(CatalogEntityKey.fromNamespaceKey(newNamespaceKey))) {
      return new DremioSchema(catalog, newNamespaceKey);
    } else {
      return null;
    }
  }

  @Override
  protected TableEntry getImplicitTable(String s, boolean b) {

    NamespaceKey newNamespaceKey = namespaceKey.getChild(s);
    DremioTable dremioTable = catalog.getTableWithSchema(newNamespaceKey);
    if (null == dremioTable) {
      return null;
    } else {
      return new TableEntryImpl(this, s, dremioTable, ImmutableList.of());
    }
  }

  @Override
  protected TableEntry getImplicitTableBasedOnNullaryFunction(String s, boolean b) {
    return null;
  }

  @Override
  protected TypeEntry getImplicitType(String name, boolean caseSensitive) {
    return null;
  }

  @Override
  protected void addImplicitSubSchemaToBuilder(
      ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
    if (builder.build().isEmpty()) {
      return;
    } else {
      throw new RuntimeException();
    }
  }

  @Override
  protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new RuntimeException();
  }

  @Override
  protected void addImplicitFunctionsToBuilder(
      ImmutableList.Builder<Function> builder, String s, boolean b) {
    throw new RuntimeException();
  }

  @Override
  protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new RuntimeException();
  }

  @Override
  protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
      ImmutableSortedMap.Builder<String, Table> builder) {
    throw new RuntimeException();
  }

  @Override
  protected void addImplicitTypeNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
    throw new RuntimeException();
  }

  @Override
  protected CalciteSchema snapshot(CalciteSchema calciteSchema, SchemaVersion schemaVersion) {
    throw new RuntimeException();
  }

  @Override
  protected boolean isCacheEnabled() {
    throw new RuntimeException();
  }

  @Override
  public void setCache(boolean b) {
    throw new RuntimeException();
  }

  @Override
  public CalciteSchema add(String s, Schema schema) {
    throw new RuntimeException();
  }
}
