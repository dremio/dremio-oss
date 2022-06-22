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
import java.util.List;
import java.util.Optional;

import com.dremio.service.autocomplete.catalog.AutocompleteSchemaProvider;
import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.Column;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class MockAutocompleteSchemaProvider extends AutocompleteSchemaProvider {

  private final List<String> context;
  private final NodeMetadata metadata;

  public MockAutocompleteSchemaProvider(List<String> context, NodeMetadata metadata) {
    super("test", null, null, null, null);
    this.context = context;
    this.metadata = metadata;
  }

  @Override
  public ImmutableSet<Column> getColumnsByFullPath(List<String> path) {
    List<String> fullPath = new ArrayList<>(context);
    fullPath.addAll(path);
    return resolveRecursive(fullPath, metadata)
      .orElseThrow(RuntimeException::new)
      .getSchema();
  }

  private Optional<NodeMetadata> resolveRecursive(List<String> path, NodeMetadata metadata) {
    if (metadata == null) {
      return Optional.empty();
    }
    if (path.isEmpty()) {
      return Optional.of(metadata);
    }
    String currentPathPart = path.get(0);
    NodeMetadata child = metadata.getChildren()
      .stream()
      .filter(c -> c.getNode().getName().equalsIgnoreCase(currentPathPart))
      .findFirst()
      .orElse(null);
    return resolveRecursive(path.subList(1, path.size()), child);
  }

  @Override
  public ImmutableList<Node> getChildrenInScope(List<String> path) {
    List<String> fullPath = new ArrayList<>(context);
    fullPath.addAll(path);
    return resolveRecursive(getPathAtCursor(fullPath), metadata)
      .map(NodeMetadata::getChildren)
      .orElse(ImmutableList.of())
      .stream()
      .map(NodeMetadata::getNode)
      .collect(ImmutableList.toImmutableList());
  }
}
