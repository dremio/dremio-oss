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
package com.dremio.dac.service.source;

import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.dac.model.resourcetree.ResourceTreeEntity;
import com.dremio.dac.model.resourcetree.ResourceTreeEntity.ResourceType;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.service.namespace.NamespaceKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Helpers for making resource tree entity list from external catalogs (e.g. Nessie) */
public final class ExternalResourceTreeUtils {
  private ExternalResourceTreeUtils() {}

  public static List<ResourceTreeEntity> generateResourceTreeEntityList(
      NamespaceKey path, Stream<ExternalNamespaceEntry> entries, ResourceType rootType) {
    Objects.requireNonNull(path);

    final String sourceName = path.getRoot();
    final List<ResourceTreeEntity> resources = new ArrayList<>();

    entries.forEach(
        entry -> {
          final String id = entry.getId();
          final String name = entry.getName();
          final List<String> fullPathList =
              Stream.concat(Stream.of(sourceName), entry.getNameElements().stream())
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
              break; // Unknown sources are ignored
            case FOLDER:
              final String url = "/resourcetree/" + path.toUrlEncodedString();
              resources.add(
                  new ResourceTreeEntity(
                      ResourceType.FOLDER,
                      name,
                      fullPathList,
                      url,
                      null,
                      versionedDatasetId,
                      rootType));
              break;
            case ICEBERG_TABLE:
              resources.add(
                  new ResourceTreeEntity(
                      ResourceType.PHYSICAL_DATASET,
                      name,
                      fullPathList,
                      null,
                      null,
                      versionedDatasetId,
                      rootType));
              break;
            case ICEBERG_VIEW:
              resources.add(
                  new ResourceTreeEntity(
                      ResourceType.VIRTUAL_DATASET,
                      name,
                      fullPathList,
                      null,
                      null,
                      versionedDatasetId,
                      rootType));
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + entry.getType());
          }
        });

    return resources;
  }
}
