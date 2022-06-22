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
package com.dremio.service.autocomplete.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.service.autocomplete.catalog.mock.MockCatalog;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTable;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTableFactory;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.autocomplete.catalog.mock.NodeMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * Factory for MockCatalogs
 */
public final class MockCatalogFactory {
  private MockCatalogFactory() {
  }

  public static MockCatalog createFromNode(NodeMetadata metadata) {
    return new MockCatalog(
      JavaTypeFactoryImpl.INSTANCE,
      ImmutableList.copyOf(getTables(ImmutableList.of(), metadata)));
  }

  private static List<MockDremioTable> getTables(ImmutableList<String> pathTokensSoFar, NodeMetadata metadata) {
    ImmutableList<String> extendedPath = ImmutableList.<String>builder()
      .addAll(pathTokensSoFar)
      .add(metadata.getNode().getName())
      .build();

    if (!metadata.getSchema().isEmpty()) {
      ImmutableList<MockSchemas.ColumnSchema> columnSchemas = ImmutableList.copyOf(metadata.getSchema()
        .stream()
        .map(schema -> MockSchemas.ColumnSchema.create(
          schema.getName(),
          schema.getType()))
        .collect(Collectors.toList()));

      // Don't include the root in the namespace tokens
      ImmutableList<String> namespaceTokens = extendedPath.subList(1, extendedPath.size());
      NamespaceKey namespaceKey = new NamespaceKey(namespaceTokens);
      MockDremioTable mockDremioTable = MockDremioTableFactory.createFromSchema(
        namespaceKey,
        JavaTypeFactoryImpl.INSTANCE,
        columnSchemas);

      return ImmutableList.of(mockDremioTable);
    }

    List<MockDremioTable> mockDremioTables = new ArrayList<>();
    for (NodeMetadata child : metadata.getChildren()) {
      mockDremioTables.addAll(getTables(extendedPath, child));
    }

    return mockDremioTables;
  }
}
