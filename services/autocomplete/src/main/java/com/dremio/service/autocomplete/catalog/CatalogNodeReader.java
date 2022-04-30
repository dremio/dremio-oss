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

import java.util.List;
import java.util.Optional;

/**
 * Reader API to let users navigate a CatalogNode tree.
 */
public abstract class CatalogNodeReader {
  public abstract HomeCatalogNode getHomeCatalogNode();

  public Optional<CatalogNode> tryGetCatalogNodeAtPath(List<String> path) {
    CatalogNode currentNode = getHomeCatalogNode();
    for (String pathToken : path) {
      if (!(currentNode instanceof CatalogNodeWithChildren)) {
        return Optional.empty();
      }

      CatalogNodeWithChildren catalogNodeWithChildren = (CatalogNodeWithChildren) currentNode;
      Optional<CatalogNode> childNode = catalogNodeWithChildren
        .getChildren()
        .stream()
        .filter(catalogNode -> catalogNode.getName().equalsIgnoreCase(pathToken))
        .findAny();

      if (!childNode.isPresent()) {
        return Optional.empty();
      }

      currentNode = childNode.get();
    }

    return Optional.of(currentNode);
  }
}
