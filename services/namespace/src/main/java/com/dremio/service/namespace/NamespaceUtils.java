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

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.HOME;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SPACE;

import java.util.List;

import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.google.common.base.Preconditions;

/**
 * Utility methods for namespace service.
 */
public final class NamespaceUtils {

  static boolean isListable(final NameSpaceContainer.Type t) {
    return (t == HOME || t == SPACE || t == FOLDER || t == SOURCE);
  }

  static boolean isPhysicalDataset(DatasetType datasetType) {
   return (datasetType == DatasetType.PHYSICAL_DATASET
     || datasetType == DatasetType.PHYSICAL_DATASET_SOURCE_FILE
     || datasetType == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
  }

  static boolean isRootEntity(Type type) {
    return type == HOME || type == SOURCE || type == SPACE;
  }

  /** helper method that returns the id of the entity in given container */
  public static String getId(NameSpaceContainer container) {
    EntityId entityId;
    switch (container.getType()) {
      case SOURCE:
        entityId = container.getSource().getId();
        break;
      case SPACE:
        entityId = container.getSpace().getId();
        break;
      case HOME:
        entityId = container.getHome().getId();
        break;
      case FOLDER:
        entityId = container.getFolder().getId();
        break;
      case DATASET:
        entityId = container.getDataset().getId();
        break;
      default:
        throw new RuntimeException("Invalid container type");
    }

    return entityId != null ? entityId.getId() : null;
  }

  /** helper method that sets the given id in given container */
  static void setId(NameSpaceContainer container, String id) {
    switch (container.getType()) {
      case SOURCE:
        container.getSource().setId(new EntityId(id));
        return;
      case SPACE:
        container.getSpace().setId(new EntityId(id));
        return;
      case HOME:
        container.getHome().setId(new EntityId(id));
        return;
      case FOLDER:
        container.getFolder().setId(new EntityId(id));
        return;
      case DATASET:
        container.getDataset().setId(new EntityId(id));
        return;
      default:
        throw new RuntimeException("Invalid container type");
    }
  }

  static<T> List<T> skipLast(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.subList(0, entitiesOnPath.size() - 1);
  }

  static<T> T lastElement(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.get(entitiesOnPath.size() - 1);
  }

  static<T> T firstElement(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.get(0);
  }
}
