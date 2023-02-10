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
package com.dremio.service.autocomplete.catalog.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Function;

import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.store.ColumnExtendedProperty;
import com.dremio.service.catalog.Table;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class MockMetadataCatalog implements SimpleCatalog<MockMetadataCatalog> {
  private final CatalogData data;

  public static class CatalogData {
    private final List<String> context;
    private final NodeMetadata head;
    private final Map<String, NodeMetadata> branches;
    private final Map<String, NodeMetadata> tags;

    public CatalogData(List<String> context,
                       NodeMetadata head,
                       Map<String, NodeMetadata> branches,
                       Map<String, NodeMetadata> tags) {
      this.context = context;
      this.head = head;
      this.branches = branches;
      this.tags = tags;
    }

    public CatalogData(List<String> context, NodeMetadata head) {
      this(context, head, ImmutableMap.of(), ImmutableMap.of());
    }

    public List<String> getContext() {
      return context;
    }

    public NodeMetadata getHead() {
      return head;
    }

    public Map<String, NodeMetadata> getBranches() {
      return branches;
    }

    public Map<String, NodeMetadata> getTags() {
      return tags;
    }
  }

  public MockMetadataCatalog(CatalogData data) {
    this.data = data;
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    return getTableSnapshot(key, TableVersionContext.LATEST_VERSION);
  }

  @Override
  public DremioTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
    NodeMetadata schemas;
    switch (context.getType()) {
      case BRANCH:
        schemas = data.branches.get(context.getValueAs(String.class));
        break;

      case TAG:
        schemas = data.tags.get(context.getValueAs(String.class));
        break;

      case LATEST_VERSION:
        schemas = data.head;
        break;

      default:
        throw new RuntimeException("Unknown type: " + context.getType());
    }

    if (schemas == null) {
      throw new RuntimeException("Could not find catalog node");
    }

    List<String> fullPath = new ArrayList<>();
    fullPath.addAll(data.getContext());
    fullPath.addAll(key.getPathComponents());

    return resolve(key, fullPath, schemas);
  }

  private DremioTable resolve(NamespaceKey key, List<String> path, NodeMetadata metadata) {
    if (metadata == null) {
      throw new RuntimeException();
    }
    if (path.isEmpty()) {
      return CatalogNodeDremioTable.create(key, metadata.getSchema());
    }
    String currentPathPart = path.get(0);
    NodeMetadata child = metadata.getChildren()
      .stream()
      .filter(c -> c.getNode().getName().equalsIgnoreCase(currentPathPart))
      .findFirst()
      .orElse(null);
    return resolve(key, path.subList(1, path.size()), child);
  }

  @Override
  public DremioTable getTableForQuery(NamespaceKey key) {
    return getTable(key);
  }

  @Override
  public void validateSelection() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    throw new RuntimeException("NOT IMPLEMENTED");
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    return ImmutableList.of();
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path,
      FunctionType functionType) {
    return ImmutableList.of();
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return null;
  }

  @Override
  public MockMetadataCatalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema) {
    return null;
  }

  @Override
  public MockMetadataCatalog resolveCatalog(CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return null;
  }

  @Override
  public MockMetadataCatalog resolveCatalog(boolean checkValidity) {
    return this;
  }

  @Override
  public MockMetadataCatalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return null;
  }

  @Override
  public MockMetadataCatalog resolveCatalog(Map<String, VersionContext> sourceVersionMapping) {
    return null;
  }

  @Override
  public DremioTable getTable(String datasetId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key) {
    return getTable(key);
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containerExists(NamespaceKey path) {
    return true;
  }

  @Override
  public Map<String, List<ColumnExtendedProperty>> getColumnExtendedProperties(DremioTable table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsVersioning(NamespaceKey namespaceKey) {
    throw new UnsupportedOperationException();
  }

  public static CatalogData createCatalog(List<String> context) {
    ImmutableMap<String, NodeMetadata> branches = MockNessieElementReader.INSTANCE
      .getBranches()
      .stream()
      .collect(ImmutableMap.toImmutableMap(b -> b.getName(), ignore -> Metadata.DEFAULT));

    ImmutableMap<String, NodeMetadata> tags = MockNessieElementReader.INSTANCE
      .getBranches()
      .stream()
      .collect(ImmutableMap.toImmutableMap(b -> b.getName(), ignore -> Metadata.DEFAULT));

    return new CatalogData(context, Metadata.DEFAULT, branches, tags);
  }
}
