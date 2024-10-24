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
package com.dremio.dac.model.namespace;

import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.dac.explore.model.Dataset;
import com.dremio.dac.model.common.Function;
import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.sources.PhysicalDataset;
import com.dremio.dac.model.sources.SourceName;
import com.dremio.dac.service.source.ExternalListResponse;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.namespace.space.proto.FolderConfig;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Helpers for making NamespaceTrees from external catalogs (e.g. Nessie) */
public final class ExternalNamespaceTreeUtils {
  private ExternalNamespaceTreeUtils() {}

  public static NamespaceTree namespaceTreeOf(
      SourceName sourceName, ExternalListResponse response, boolean includeUDFChildren) {
    Objects.requireNonNull(sourceName);

    final NamespaceTree namespaceTree = new NamespaceTree();
    namespaceTree.setNextPageToken(response.getPageToken());
    Stream<ExternalNamespaceEntry> entries = response.getEntriesStream();
    entries.forEach(
        entry -> {
          final String id = entry.getId();
          final String name = entry.getName();
          final List<String> namespace = entry.getNamespace();
          final List<String> fullPathList =
              Stream.concat(Stream.of(sourceName.getName()), entry.getNameElements().stream())
                  .collect(Collectors.toList());
          final TableVersionContext tableVersionContext = entry.getTableVersionContext();
          final String versionedDatasetId =
              (id == null || tableVersionContext == null)
                  ? null
                  : VersionedDatasetId.newBuilder()
                      .setTableKey(fullPathList)
                      .setContentId(id)
                      .setTableVersionContext(tableVersionContext)
                      .build()
                      .asString();

          switch (entry.getType()) {
            case UNKNOWN:
              break; // Unknown types are ignored
            case UDF:
              if (includeUDFChildren) {
                namespaceTree.addFunction(
                    Function.newInstance(
                        (versionedDatasetId == null)
                            ? UUID.randomUUID().toString()
                            : versionedDatasetId,
                        fullPathList));
              }
              break;
            case FOLDER:
              namespaceTree.addFolder(
                  Folder.newInstance(
                      sourceName,
                      new FolderConfig().setFullPathList(fullPathList).setName(name),
                      versionedDatasetId));
              break;
            case ICEBERG_TABLE:
              namespaceTree.addPhysicalDataset(
                  PhysicalDataset.newInstance(sourceName, namespace, name, versionedDatasetId));
              break;
            case ICEBERG_VIEW:
              namespaceTree.addDataset(
                  Dataset.newInstance(sourceName, namespace, name, versionedDatasetId));
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + entry.getType());
          }
        });

    return namespaceTree;
  }
}
