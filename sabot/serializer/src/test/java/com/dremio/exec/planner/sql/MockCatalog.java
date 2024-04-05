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
package com.dremio.exec.planner.sql;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.catalog.TableMetadataVerifyRequest;
import com.dremio.exec.catalog.TableMetadataVerifyResult;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.service.catalog.Table;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;

/** A catalog that has a fixed list of known tables. */
public class MockCatalog implements SimpleCatalog<MockCatalog> {
  private final RelDataTypeFactory typeFactory;
  private final TableSet tableSet;

  public MockCatalog(RelDataTypeFactory typeFactory, ImmutableList<MockDremioTable> knownTables) {
    Preconditions.checkNotNull(typeFactory);
    Preconditions.checkNotNull(knownTables);

    this.typeFactory = typeFactory;
    this.tableSet = new TableSet();
    for (MockDremioTable mockDremioTable : knownTables) {
      this.tableSet.put(mockDremioTable);
    }
  }

  protected RelDataTypeFactory getTypeFactory() {
    return this.typeFactory;
  }

  protected TableSet getTableSet() {
    return this.tableSet;
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    Optional<MockDremioTable> tryGetTable = this.tableSet.tryGetValue(key);
    if (tryGetTable.isPresent()) {
      return tryGetTable.get();
    }

    throw new RuntimeException("Unknown table " + key.getName());
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return getTable(key);
  }

  @Override
  public DremioTable getTableSnapshotForQuery(CatalogEntityKey catalogEntityKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DremioTable getTableSnapshot(CatalogEntityKey catalogEntityKey) {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public Optional<TableMetadataVerifyResult> verifyTableMetadata(
      CatalogEntityKey key, TableMetadataVerifyRequest metadataVerifyRequest) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DremioTable getTable(CatalogEntityKey catalogEntityKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    return this.tableSet.values().stream()
        .map(t -> t.getPath().getParent().toUnescapedString())
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    return ImmutableList.of();
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path, FunctionType functionType) {
    return ImmutableList.of();
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return null;
  }

  @Override
  public MockCatalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return null;
  }

  @Override
  public MockCatalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return null;
  }

  @Override
  public DremioTable getTable(java.lang.String datasetId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return getTable(key);
  }

  @Override
  public DremioTable getTableNoResolve(CatalogEntityKey catalogEntityKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containerExists(CatalogEntityKey path) {
    return true;
  }

  @Override
  public Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table) {
    throw new UnsupportedOperationException();
  }

  /** KV store for MockDremioTables by their normalized namespace key. */
  public static final class TableSet {
    private final Map<NamespaceKey, MockDremioTable> tables;

    public TableSet() {
      this.tables = new HashMap<>();
    }

    public Optional<MockDremioTable> tryGetValue(NamespaceKey namespaceKey) {
      Preconditions.checkNotNull(namespaceKey);
      NamespaceKey normalizedKey = namespaceKey.asLowerCase();
      if (tables.containsKey(normalizedKey)) {
        return Optional.of(tables.get(normalizedKey));
      }

      return Optional.empty();
    }

    public MockDremioTable put(MockDremioTable table) {
      Preconditions.checkNotNull(table);
      NamespaceKey normalizedKey = table.getPath().asLowerCase();
      return this.tables.put(normalizedKey, table);
    }

    public Collection<MockDremioTable> values() {
      return this.tables.values();
    }
  }
}
