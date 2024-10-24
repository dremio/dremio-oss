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
package com.dremio.service.namespace;

import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FOLDER;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.FUNCTION;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.HOME;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SOURCE;
import static com.dremio.service.namespace.proto.NameSpaceContainer.Type.SPACE;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Utility methods for namespace service. */
public final class NamespaceUtils {

  public static boolean isListable(final NameSpaceContainer.Type t) {
    return (t == HOME || t == SPACE || t == FOLDER || t == SOURCE || t == FUNCTION);
  }

  public static boolean isPhysicalDataset(DatasetType datasetType) {
    return (datasetType == DatasetType.PHYSICAL_DATASET
        || datasetType == DatasetType.PHYSICAL_DATASET_SOURCE_FILE
        || datasetType == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER);
  }

  /** helper method that returns the id of the entity in given container */
  public static String getIdOrNull(NameSpaceContainer container) {
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
      case FUNCTION:
        entityId = container.getFunction().getId();
        break;
      default:
        throw new RuntimeException("Invalid container type");
    }

    return entityId != null ? entityId.getId() : null;
  }

  /** helper method that sets the given id in given container */
  public static void setId(NameSpaceContainer container, String id) {
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
      case FUNCTION:
        container.getFunction().setId(new EntityId(id));
        return;
      default:
        throw new RuntimeException("Invalid container type");
    }
  }

  /** helper method that returns the tag of the entity in given container */
  public static String getTag(NameSpaceContainer container) {
    switch (container.getType()) {
      case SOURCE:
        return container.getSource().getTag();
      case SPACE:
        return container.getSpace().getTag();
      case HOME:
        return container.getHome().getTag();
      case FOLDER:
        return container.getFolder().getTag();
      case DATASET:
        return container.getDataset().getTag();
      case FUNCTION:
        return container.getFunction().getTag();
      default:
        throw new RuntimeException("Invalid container type");
    }
  }

  public static NamespaceKey getKey(NameSpaceContainer container) {
    return new NamespaceKey(container.getFullPathList());
  }

  static <T> List<T> skipLast(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.subList(0, entitiesOnPath.size() - 1);
  }

  public static <T> T lastElement(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.get(entitiesOnPath.size() - 1);
  }

  static <T> T firstElement(List<T> entitiesOnPath) {
    Preconditions.checkArgument(entitiesOnPath.size() >= 1);
    return entitiesOnPath.get(0);
  }

  /**
   * Carry over few properties from old dataset config to new one
   *
   * @param oldConfig old dataset config from namespace
   * @param newConfig new dataset config thats about to be saved in namespace
   */
  public static void copyFromOldConfig(DatasetConfig oldConfig, DatasetConfig newConfig) {
    if (oldConfig == null) {
      return;
    }
    newConfig.setId(oldConfig.getId());
    newConfig.setTag(oldConfig.getTag());
    newConfig.setCreatedAt(oldConfig.getCreatedAt());
    newConfig.setType(oldConfig.getType());
    newConfig.setFullPathList(oldConfig.getFullPathList());
    newConfig.setOwner(oldConfig.getOwner());
    // make sure to copy the acceleration settings from old to new config
    // newConfig may contain upgrade fileFormat physical settings
    if (oldConfig.getPhysicalDataset() != null) {
      if (newConfig.getPhysicalDataset() == null) {
        newConfig.setPhysicalDataset(new PhysicalDataset());
      }
    }
  }

  public static String getVersion(NamespaceKey namespaceKey, NamespaceService namespaceService)
      throws NamespaceException {
    NameSpaceContainer container = namespaceService.getEntities(Arrays.asList(namespaceKey)).get(0);

    switch (container.getType()) {
      case SOURCE:
        return container.getSource().getTag();
      case SPACE:
        return container.getSpace().getTag();
      case HOME:
        return container.getHome().getTag();
      case FOLDER:
        return container.getFolder().getTag();
      case DATASET:
        return container.getDataset().getTag();
      default:
        throw new RuntimeException("Invalid container type");
    }
  }

  /**
   * Get the attribute list if not null, otherwise creates new list and sets it on the
   * nameSpaceContainer.
   *
   * @param nameSpaceContainer nameSpaceContainer
   * @return given list if not null, or else, a new array list
   */
  public static List<com.dremio.common.Any> getOrCreateAttributeList(
      NameSpaceContainer nameSpaceContainer) {
    if (null == nameSpaceContainer.getAttributesList()) {
      nameSpaceContainer.setAttributesList(new ArrayList<>());
    }
    return nameSpaceContainer.getAttributesList();
  }

  /**
   * Get the given list if not null, or else, an immutable empty list.
   *
   * @param list list
   * @param <T> entity type
   * @return given list if not null, or else, an immutable empty list
   */
  public static <T> List<T> getOrEmptyList(List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

  public static boolean isRestrictedInternalSource(NameSpaceContainer rootEntity) {
    if (rootEntity.getType() != NameSpaceContainer.Type.SOURCE) {
      return false;
    }

    final List<String> rootFullPathList = rootEntity.getFullPathList();
    return "INFORMATION_SCHEMA".equals(rootEntity.getSource().getType())
        || "ESYS".equals(rootEntity.getSource().getType())
        || "ESYSFLIGHT".equals(rootEntity.getSource().getType())
        || ("INTERNAL".equals(rootEntity.getSource().getType())
            && "$scratch".equals(rootEntity.getSource().getName()))
        || rootFullPathList.get(0).startsWith("__");
  }

  public static boolean isACLRestrictedInternalSource(NameSpaceContainer rootEntity) {
    if (rootEntity.getType() != NameSpaceContainer.Type.SOURCE) {
      return false;
    }
    final List<String> rootFullPathList = rootEntity.getFullPathList();
    return "INFORMATION_SCHEMA".equals(rootEntity.getSource().getType())
        || ("INTERNAL".equals(rootEntity.getSource().getType())
            && "$scratch".equals(rootEntity.getSource().getName()))
        || rootFullPathList.get(0).startsWith("__");
  }

  public static boolean isSystemTable(NameSpaceContainer rootEntity) {
    if (rootEntity.getType() != NameSpaceContainer.Type.SOURCE) {
      return false;
    }
    return "ESYS".equals(rootEntity.getSource().getType())
        || "ESYSFLIGHT".equals(rootEntity.getSource().getType());
  }

  public static boolean isSchemaOutdated(DatasetConfig datasetConfig) {
    return datasetConfig.getType() == DatasetType.VIRTUAL_DATASET
        && datasetConfig.getVirtualDataset() != null
        && datasetConfig.getVirtualDataset().getSchemaOutdated() == Boolean.TRUE;
  }
}
