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
package com.dremio.service.namespace;

import static com.dremio.common.utils.Protos.listNotNull;
import static com.dremio.common.utils.Protos.notEmpty;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ALLPARENTS;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_COLUMNS_NAMES;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_ID;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_OWNER;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_PARENTS;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_SOURCES;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_SQL;
import static com.dremio.service.namespace.DatasetIndexKeys.DATASET_UUID;

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
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Namespace search indexing. for now only support virtual and physical dataset indexing.
 */
public class NamespaceConverter implements DocumentConverter <byte[], NameSpaceContainer> {
  @Override
  public void convert(DocumentWriter writer, byte[] key, NameSpaceContainer container) {
    if (container.getType() != Type.DATASET) {
      return;
    }

    final DatasetConfig datasetConfig = container.getDataset();
    writer.write(DATASET_ID, new NamespaceKey(container.getFullPathList()).getSchemaPath());
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
  }

  private void addColumns(DocumentWriter writer, DatasetConfig datasetConfig) {
    final ByteString schemaBytes = DatasetHelper.getSchemaBytes(datasetConfig);
    if (schemaBytes != null) {
      Schema schema = Schema.getRootAsSchema(schemaBytes.asReadOnlyByteBuffer());
      org.apache.arrow.vector.types.pojo.Schema s = org.apache.arrow.vector.types.pojo.Schema.convertSchema(schema);
      final String[] columns = new String[s.getFields().size()];
      int i = 0;
      for (Field field : s.getFields()) {
        columns[i++] = field.getName();
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
            columns[i++] = field.getName();
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
