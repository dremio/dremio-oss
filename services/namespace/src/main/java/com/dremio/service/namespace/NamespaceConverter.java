/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.namespace;

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.common.utils.Protos.notEmpty;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ALLPARENTS;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_COLUMNS_NAMES;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_OWNER;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_PARENTS;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_SOURCES;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_SQL;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_UUID;
import static com.dremio.service.namespace.DatasetIndexKeys.UNQUOTED_LC_NAME;
import static com.dremio.service.namespace.DatasetIndexKeys.UNQUOTED_LC_SCHEMA;
import static com.dremio.service.namespace.DatasetIndexKeys.UNQUOTED_NAME;
import static com.dremio.service.namespace.DatasetIndexKeys.UNQUOTED_SCHEMA;

import java.util.List;
import java.util.Set;

import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.KVStoreProvider.DocumentConverter;
import com.dremio.datastore.KVStoreProvider.DocumentWriter;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Namespace search indexing. for now only support pds, vds, source and space indexing.
 */
public class NamespaceConverter implements DocumentConverter <byte[], NameSpaceContainer> {
  @Override
  public void convert(DocumentWriter writer, byte[] key, NameSpaceContainer container) {
    writer.write(NamespaceIndexKeys.ENTITY_TYPE, container.getType().getNumber());

    final NamespaceKey nkey = new NamespaceKey(container.getFullPathList());
    NamespaceKey lkey = nkey.asLowerCase();
    writer.write(NamespaceIndexKeys.UNQUOTED_LC_PATH, lkey.toUnescapedString());

    // add standard  and lower case searches.
    writer.write(UNQUOTED_NAME, nkey.getName());
    writer.write(UNQUOTED_LC_NAME, lkey.getName());

    if (container.getType() == NameSpaceContainer.Type.DATASET) {
      writer.write(UNQUOTED_SCHEMA, nkey.getParent().toUnescapedString());
      writer.write(UNQUOTED_LC_SCHEMA, lkey.getParent().toUnescapedString());
    } else {
      writer.write(UNQUOTED_SCHEMA, nkey.toUnescapedString());
      writer.write(UNQUOTED_LC_SCHEMA, lkey.toUnescapedString());
    }

    switch (container.getType()) {
      case DATASET: {
        final DatasetConfig datasetConfig = container.getDataset();

        writer.write(DatasetIndexKeys.DATASET_ID, new NamespaceKey(container.getFullPathList()).getSchemaPath());

        writer.write(DATASET_UUID, datasetConfig.getId().getId());
        if (datasetConfig.getOwner() != null) {
          writer.write(DATASET_OWNER, datasetConfig.getOwner());
        }
        switch (datasetConfig.getType()) {
          case VIRTUAL_DATASET: {

            final VirtualDataset virtualDataset = datasetConfig.getVirtualDataset();

            writer.write(DATASET_SQL, virtualDataset.getSql());

            addParents(writer, virtualDataset.getParentsList());
            addColumns(writer, datasetConfig);
            addSourcesAndOrigins(writer, virtualDataset.getFieldOriginsList());
            addAllParents(writer, virtualDataset.getParentsList(), virtualDataset.getGrandParentsList());
          }
          break;

          case PHYSICAL_DATASET:
          case PHYSICAL_DATASET_SOURCE_FILE:
          case PHYSICAL_DATASET_SOURCE_FOLDER: {
            addColumns(writer, datasetConfig);
            // TODO index physical dataset properties
          }
          break;

          default:
            break;
        }
        break;
      }

      case HOME: {
        HomeConfig homeConfig = container.getHome();
        writer.write(NamespaceIndexKeys.HOME_ID, homeConfig.getId().getId());
        break;
      }

      case SOURCE: {
        final SourceConfig sourceConfig = container.getSource();
        writer.write(NamespaceIndexKeys.SOURCE_ID, sourceConfig.getId().getId());
        break;
      }

      case SPACE: {
        final SpaceConfig spaceConfig = container.getSpace();
        writer.write(NamespaceIndexKeys.SPACE_ID, spaceConfig.getId().getId());
        break;
      }

      case FOLDER: {
        final FolderConfig folderConfig = container.getFolder();
        writer.write(NamespaceIndexKeys.FOLDER_ID, folderConfig.getId().getId());
        break;
      }

      default:
        break;
    }
  }

  private void addColumns(DocumentWriter writer, DatasetConfig datasetConfig) {
    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
    if (schemaBytes != null) {
      Schema schema = Schema.getRootAsSchema(schemaBytes.asReadOnlyByteBuffer());
      org.apache.arrow.vector.types.pojo.Schema s = org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
      final String[] columns = new String[s.getFields().size()];
      int i = 0;
      for (Field field : s.getFields()) {
        columns[i++] = field.getName().toLowerCase();
      }
      writer.write(DATASET_COLUMNS_NAMES, columns);
    } else {
      // If virtual dataset was created with view fields
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        final List<ViewFieldType> viewFieldTypes = datasetConfig.getVirtualDataset().getSqlFieldsList();
        if (notEmpty(viewFieldTypes)) {
          final String[] columns = new String[viewFieldTypes.size()];
          int i = 0;
          for (ViewFieldType field : viewFieldTypes) {
            columns[i++] = field.getName().toLowerCase();
          }
          writer.write(DATASET_COLUMNS_NAMES, columns);
        }
      }
    }
  }

  private void addParents(DocumentWriter writer, List<ParentDataset> parentDatasetList) {
    if (notEmpty(parentDatasetList)) {
      final String[] parents = new String[parentDatasetList.size()];
      int i = 0;
      for (ParentDataset parent : parentDatasetList) {
        parents[i++] = PathUtils.constructFullPath(parent.getDatasetPathList());
      }
      writer.write(DATASET_PARENTS, parents);
    }
  }


  private void addSourcesAndOrigins(DocumentWriter writer, List<FieldOrigin> fieldOrigins) {
    if (notEmpty(fieldOrigins)) {
      Set<String> sources = Sets.newHashSet();
      final List<String> empty = ImmutableList.of();
      for (FieldOrigin fieldOrigin : fieldOrigins) {
        for (Origin origin : listNotNull(fieldOrigin.getOriginsList())) {
          // DX-3999: fix this in calcite
          final List<String> path = Optional.fromNullable(origin.getTableList()).or(empty);
          if (path.isEmpty()) {
            continue;
          }
          NamespaceKey dataset = new NamespaceKey(origin.getTableList());
          sources.add(dataset.getRoot());
        }
      }
      writer.write(DATASET_SOURCES, sources.toArray(new String[0]));
    }
  }

  private void addAllParents(DocumentWriter writer, List<ParentDataset> parents, List<ParentDataset> grandParents) {
    if (notEmpty(parents)) {
      grandParents = listNotNull(grandParents);
      int i = 0;
      final String[] allParents = new String[parents.size() + grandParents.size()];
      for (ParentDataset parent : parents) {
        allParents[i++] = PathUtils.constructFullPath(parent.getDatasetPathList());
      }
      for (ParentDataset grandParent : grandParents) {
        allParents[i++] = PathUtils.constructFullPath(grandParent.getDatasetPathList());
      }
      writer.write(DATASET_ALLPARENTS, allParents);
    }
  }
}
