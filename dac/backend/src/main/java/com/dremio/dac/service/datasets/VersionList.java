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
package com.dremio.dac.service.datasets;

import com.dremio.dac.proto.model.dataset.NameDatasetRef;
import com.dremio.dac.proto.model.dataset.VirtualDatasetVersion;
import com.dremio.datastore.api.Document;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for all versions of a dataset. Constructs linked list, checks for validity of the list,
 * etc.
 */
final class VersionList {
  private static final Logger logger = LoggerFactory.getLogger(VersionList.class);

  private final ImmutableList<
          ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
      linkedLists;
  private final ImmutableSet<DatasetVersionMutator.VersionDatasetKey> unusableKeys;

  private VersionList(
      ImmutableList<
              ImmutableList<
                  Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
          linkedLists,
      ImmutableSet<DatasetVersionMutator.VersionDatasetKey> unusableKeys) {
    this.linkedLists = linkedLists;
    this.unusableKeys = unusableKeys;
  }

  /** These keys can be deleted. */
  ImmutableSet<DatasetVersionMutator.VersionDatasetKey> getUnusableKeys() {
    return unusableKeys;
  }

  Optional<ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
      getVersionsList(DatasetVersionMutator.VersionDatasetKey headVersionKey) {
    for (ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
        list : linkedLists) {
      // Normal case: there is a list matching the version stored in DatasetConfig.
      if (list.get(0).getKey().equals(headVersionKey)) {
        return Optional.of(list);
      }
    }

    // There is no matching list: possibly concurrent modification.
    return Optional.empty();
  }

  Optional<ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
      getFirstVersionsList() {
    return linkedLists.isEmpty() ? Optional.empty() : Optional.of(linkedLists.get(0));
  }

  /** Constructs {@link VersionList} with possibly multiple candidate linked lists. */
  static VersionList build(
      Collection<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
          allVersions) {
    // Allocate linked list nodes.
    ImmutableMap<NameDatasetRef, VersionListNode> nodesByRef =
        allVersions.stream()
            .map(VersionListNode::new)
            .collect(
                ImmutableMap.toImmutableMap(
                    node ->
                        new NameDatasetRef(node.document.getKey().getPath().toString())
                            .setDatasetVersion(node.document.getKey().getVersion().getVersion()),
                    Functions.identity()));

    // Set next and previous in the list nodes.
    for (VersionListNode node : nodesByRef.values()) {
      NameDatasetRef previousVersion = node.document.getValue().getPreviousVersion();
      if (previousVersion != null) {
        VersionListNode next = nodesByRef.get(previousVersion);
        node.next = next;
        if (next != null) {
          next.previous = node;
        }
      }
    }

    // Find list head(s) and construct linked list(s) from them, accumulate keys that cannot be used
    // because of a loop.
    ImmutableList.Builder<
            ImmutableList<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>>
        linkedListsBuilder = ImmutableList.builder();
    ImmutableSet.Builder<DatasetVersionMutator.VersionDatasetKey> unusableKeysBuilder =
        ImmutableSet.builder();
    nodesByRef.values().stream()
        .filter(n -> n.previous == null)
        .forEach(
            head -> {
              ImmutableList<
                      Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
                  list = buildLinkedList(head, allVersions.size(), unusableKeysBuilder);
              if (!list.isEmpty()) {
                linkedListsBuilder.add(list);
              }
            });
    return new VersionList(linkedListsBuilder.build(), unusableKeysBuilder.build());
  }

  /** Attempts to build a linked list from a head node. */
  private static ImmutableList<
          Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
      buildLinkedList(
          VersionListNode headNode,
          int limit,
          ImmutableSet.Builder<DatasetVersionMutator.VersionDatasetKey> unusableKeysBuilder) {
    Preconditions.checkArgument(headNode.previous == null, "Passed headNode is not a head node.");

    // Traverse the list from the head. Defend against loops in the list.
    ImmutableList.Builder<Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion>>
        resultBuilder = ImmutableList.builder();
    VersionListNode node = headNode;
    int size = 0;
    do {
      resultBuilder.add(node.document);
      size++;
      node = node.next;
    } while (node != null && size < limit + 1);

    // Check size, if it doesn't match the number of versions, the list has a loop, bail out.
    if (size > limit) {
      // Add keys from the list with loop to the list of unusable keys.
      for (Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> document :
          resultBuilder.build()) {
        unusableKeysBuilder.add(document.getKey());
      }
      return ImmutableList.of();
    }
    return resultBuilder.build();
  }

  /** Dataset version linked list node. */
  private static final class VersionListNode {
    private final Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> document;
    private VersionListNode previous;
    private VersionListNode next;

    private VersionListNode(
        Document<DatasetVersionMutator.VersionDatasetKey, VirtualDatasetVersion> document) {
      this.document = document;
    }
  }
}
