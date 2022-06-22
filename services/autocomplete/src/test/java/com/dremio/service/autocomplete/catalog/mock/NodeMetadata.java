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

import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.Column;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public final class NodeMetadata {
  private final Node node;
  private final ImmutableList<NodeMetadata> children;
  private final ImmutableSet<Column> schema;

  private NodeMetadata(Node node, ImmutableList<NodeMetadata> children, ImmutableSet<Column> schema) {
    this.node = node;
    this.children = children;
    this.schema = schema;
  }

  public static NodeMetadata file(String name) {
    return new NodeMetadata(new Node(name, Node.Type.FILE), ImmutableList.of(), ImmutableSet.of());
  }

  public static NodeMetadata dataset(Node node, ImmutableSet<Column> schema) {
    return new NodeMetadata(node, ImmutableList.of(), schema);
  }

  public static NodeMetadata pathNode(Node node, NodeMetadata... children) {
    return new NodeMetadata(node, ImmutableList.copyOf(children), ImmutableSet.of());
  }

  public Node getNode() {
    return node;
  }

  public ImmutableList<NodeMetadata> getChildren() {
    return children;
  }

  public ImmutableSet<Column> getSchema() {
    return schema;
  }
}
