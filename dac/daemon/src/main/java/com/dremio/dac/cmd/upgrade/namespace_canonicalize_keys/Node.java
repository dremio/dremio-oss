/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade.namespace_canonicalize_keys;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * represents an entity in the namespace. Makes it easy to traverse all entities
 * according to their path relationships. Abstract the logic to update/rename
 * entities depending on their type
 */
abstract class Node {

  enum NodeType {
    ROOT,
    HOME,
    SPACE,
    SOURCE,
    FOLDER,
    DATASET
  }

  private final NodeType type;
  private final List<String> path = Lists.newArrayList();
  private String name;

  private final List<Node> children = Lists.newArrayList();

  Node(NodeType type, String name) {
    this(type, name, Collections.singletonList(name));
  }

  Node(NodeType type, String name, List<String> path) {
    this.type = type;
    this.name = name;
    this.path.addAll(path);
  }

  List<String> getPath() {
    return path;
  }

  String getName() {
    return name;
  }

  public String fullPath() {
    return PathUtils.constructFullPath(path);
  }

  List<Node> getChildren() {
    return children;
  }

  boolean isAncestorOf(Node node) {
    return node.fullPath().startsWith(fullPath() + ".");
  }

  boolean isParentOf(Node node) {
    return node.path.size() == path.size() + 1;
  }

  void addChild(Node node) {
    // make sure this is a parent of node
    Preconditions.checkState(isAncestorOf(node), String.format("%s is not a child of %s", node.fullPath(), fullPath()));

    if (isParentOf(node)) {
      // node is a direct descendant of this
      children.add(node);
    } else {
      boolean foundParent = false;
      for(Node child : children) {
        if (child.isAncestorOf(node)) {
          child.addChild(node);
          foundParent = true;
          break;
        }
      }
      Preconditions.checkState(foundParent,
        String.format("couldn't exploreAndAdd %s to %s, no parent node found", node.fullPath(), fullPath()));
    }
  }

  public NamespaceKey toNamespaceKey() {
    return new NamespaceKey(path);
  }

  abstract long getVersion();

  void print(PrintWriter writer, String indent, boolean printChildren) {
    writer.printf("%s%s %s [%s] version %d%n", indent, type, name, fullPath(), getVersion());
    if (printChildren) {
      for (Node child : children) {
        child.print(writer, indent + "  ", true);
      }
    }
  }

  void print(PrintWriter writer) {
    print(writer, "", true);
  }

  public String toString(boolean printChildren) {
    final StringWriter writer = new StringWriter();
    print(new PrintWriter(writer), "", printChildren);
    return writer.toString();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /**
   * @return conflicting children nodes for current node
   */
  private List<Node> getConflictingChildren() {
    List<Node> conflictingNodes = Lists.newArrayList();
    // index children by name
    ListMultimap<String, Node> nameMap = FluentIterable
      .from(children)
      .index(new Function<Node, String>() {
      @Override
      public String apply(Node child) {
        return child.name.toLowerCase();
      }
    });
    // for each name, find conflicting children
    for (String name : nameMap.keySet()) {
      final List<Node> nodes = nameMap.get(name);
      if (nodes.size() > 1) {
        conflictingNodes.addAll(nodes);
      }
    }
    return conflictingNodes;
  }

  public Collection<Node> findDuplicates() {
    FluentIterable<Node> duplicates = FluentIterable.from(getConflictingChildren());
    for (Node child : children) {
      duplicates = duplicates.append(child.findDuplicates());
    }
    return duplicates.toList();
  }

  abstract void saveTo(NamespaceService ns, UpgradeContext context) throws NamespaceException;

  public void normalize(UpgradeContext context) throws NamespaceException {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider().get());
    final NamespaceService oldNamespaceService = new NamespaceServiceWithoutNormalization(context.getKVStoreProvider().get());

    final boolean normalize = !fullPath().toLowerCase().equals(fullPath());

    if (normalize) {
      saveTo(namespaceService, context);
    }

    for (Node child : children) {
      child.normalize(context);
    }

    if (normalize) {
      oldNamespaceService.deleteEntity(toNamespaceKey());
    }
  }

}
