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

import java.util.Comparator;

import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

/**
 * Helper methods
 */
class Utils {

  private static Iterable<FolderConfig> getFolders(NamespaceService ns, NamespaceKey parent) throws NamespaceException {
    return FluentIterable
      .from(ns.getAllFolders(parent))
      .toSortedList(new Comparator<FolderConfig>() {
        @Override
        public int compare(FolderConfig o1, FolderConfig o2) {
          return Integer.compare(o1.getFullPathList().size(), o2.getFullPathList().size());
        }
      });
  }

  private static Iterable<DatasetConfig> getDatasets(final NamespaceService ns, NamespaceKey parent) throws NamespaceException {
    return FluentIterable
      .from(ns.getAllDatasets(parent))
      .transform(new Function<NamespaceKey, DatasetConfig>() {
        @Override
        public DatasetConfig apply(NamespaceKey key) {
          try {
            return ns.getDataset(key);
          } catch (NamespaceException e) {
            throw Throwables.propagate(e);
          }
        }
      })
      .toSortedList(new Comparator<DatasetConfig>() {
        @Override
        public int compare(DatasetConfig o1, DatasetConfig o2) {
          return Integer.compare(o1.getFullPathList().size(), o2.getFullPathList().size());
        }
      });
  }

  private static void exploreAndAdd(RootNode root, Node node, NamespaceService ns) throws NamespaceException {
    root.addChild(node);

    // retrieve all folders from home and sort them by their path size
    // so we make sure to insert the parent folders first
    for (FolderConfig folder : getFolders(ns, node.toNamespaceKey())) {
      node.addChild(new FolderNode(folder));
    }

    for (DatasetConfig dataset : getDatasets(ns, node.toNamespaceKey())) {
      node.addChild(new DatasetNode(dataset));
    }
  }

  static RootNode buildNamespaceHierarchy(NamespaceService ns) throws NamespaceException {

    RootNode rootNode = new RootNode();

    for (HomeConfig home : ns.getHomeSpaces()) {
      final HomeNode node = new HomeNode(home);
      exploreAndAdd(rootNode, node, ns);
    }

    for (SourceConfig source : ns.getSources()) {
      final SourceNode node = new SourceNode(source);
      exploreAndAdd(rootNode, node, ns);
    }

    for (SpaceConfig space : ns.getSpaces()) {
      final SpaceNode node = new SpaceNode(space);
      exploreAndAdd(rootNode, node, ns);
    }

    return rootNode;
  }

}
