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
package com.dremio.service.autocomplete.statements.visitors;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.statements.grammar.DropStatement;

public final class DropStatementCompletionProvider {
  private DropStatementCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    DropStatement dropStatement,
    AutocompleteEngineContext autocompleteEngineContext) {

    List<Node> nodes = autocompleteEngineContext
      .getAutocompleteSchemaProvider()
      .getChildrenInScope(dropStatement.getCatalogPath().getPathTokens());

    return Completions
      .builder()
      .addCatalogNodes(filterIrrelevantNodes(nodes, dropStatement.getDropType()))
      .build();
  }

  private static List<Node> filterIrrelevantNodes(List<Node> nodes, DropStatement.Type type) {
    switch (type) {
      case VIEW:
      case VDS:
        return nodes.stream()
          .filter(DropStatementCompletionProvider::isPartOfVirtualDatasourcePath)
          .collect(Collectors.toList());

      case TABLE:
        return nodes.stream()
          .filter(DropStatementCompletionProvider::isPartOfPhysicalDatasourcePath)
          .collect(Collectors.toList());
      default:
        return nodes;
    }
  }

  private static boolean isPartOfPhysicalDatasourcePath(Node node) {
    switch (node.getType()) {
      case VIRTUAL_SOURCE:
      case FILE:
        return false;
      default:
        return true;
    }
  }

  private static boolean isPartOfVirtualDatasourcePath(Node node) {
    switch (node.getType()) {
      case PHYSICAL_SOURCE:
      case FILE:
        return false;
      default:
        return true;
    }
  }
}
