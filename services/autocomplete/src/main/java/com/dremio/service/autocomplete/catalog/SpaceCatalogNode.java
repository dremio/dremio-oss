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

import com.google.common.collect.ImmutableList;

/**
 * A CatalogNode representing a space.
 */
public final class SpaceCatalogNode extends CatalogNodeWithChildren {
  public SpaceCatalogNode(String name, ImmutableList<CatalogNode> children) {
    super(name, children);
  }

  @Override
  public <R> R accept(CatalogNodeVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
