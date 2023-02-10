
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

import java.util.stream.Collectors;

import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.Column;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


/**
 * A mocked out CatalogNode.
 */
public final class Metadata {
  private static final NodeMetadata[] folderChildren = new NodeMetadata[]{
    NodeMetadata.file("file"),
    NodeMetadata.pathNode(
      new Node("source", Node.Type.SOURCE),
      NodeMetadata.file("source file")),
    NodeMetadata.dataset(new Node("physical dataset", Node.Type.PHYSICAL_SOURCE), ImmutableSet.of()),
    NodeMetadata.dataset(new Node("virtual dataset", Node.Type.VIRTUAL_SOURCE), ImmutableSet.of()),
    NodeMetadata.dataset(new Node("EMP", Node.Type.PHYSICAL_SOURCE), createSchema(MockSchemas.EMP)),
    NodeMetadata.dataset(new Node("DEPT", Node.Type.PHYSICAL_SOURCE), createSchema(MockSchemas.DEPT)),
    NodeMetadata.dataset(new Node("SALGRADE", Node.Type.VIRTUAL_SOURCE), createSchema(MockSchemas.SALGRADE))
  };

  public static final NodeMetadata DEFAULT = NodeMetadata.pathNode(
    new Node("@dremio", Node.Type.HOME),
    NodeMetadata.pathNode(
      new Node("space", Node.Type.SPACE),
      NodeMetadata.pathNode(new Node("folder", Node.Type.FOLDER), folderChildren)
    ),
    NodeMetadata.pathNode(
      new Node("space with a space in the name", Node.Type.SPACE),
      NodeMetadata.pathNode(new Node("folder with a space in the name", Node.Type.FOLDER), folderChildren)
    ),
    NodeMetadata.pathNode(
      new Node("@space", Node.Type.SPACE),
      NodeMetadata.pathNode(new Node("@folder", Node.Type.FOLDER), folderChildren)
    ));

  private Metadata() {
  }

  private static ImmutableSet<Column> createSchema(ImmutableList<MockSchemas.ColumnSchema> mockSchema) {
    return ImmutableSet.copyOf(mockSchema
      .stream()
      .map(mockColumnSchema -> Column.typedColumn(mockColumnSchema.getName(), mockColumnSchema.getSqlTypeName()))
      .collect(Collectors.toList()));
  }
}
