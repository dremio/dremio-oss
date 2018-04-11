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
package com.dremio.dac.api;

import java.util.List;

import com.dremio.dac.model.spaces.HomeName;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

/**
 * Catalog Item
 */
public class CatalogItem {
  /**
   * Catalog Item Type
   */
  public enum CatalogItemType { DATASET, CONTAINER, FILE }

  /**
   * Catalog Container Sub Type
   */
  public enum ContainerSubType { SPACE, SOURCE, FOLDER, HOME }

  /**
   * Catalog Dataset Sub Type
   */
  public enum DatasetSubType { VIRTUAL, PROMOTED, DIRECT }

  private final String id;
  private final List<String> path;
  private final CatalogItemType type;
  private final String tag;
  private final DatasetSubType datasetType;
  private final ContainerSubType containerType;

  @JsonCreator
  public CatalogItem(
    @JsonProperty("id") String id,
    @JsonProperty("path") List<String> path,
    @JsonProperty("tag") String tag,
    @JsonProperty("type") CatalogItemType type,
    @JsonProperty("datasetType") DatasetSubType datasetType,
    @JsonProperty("containerType") ContainerSubType containerType
  ) {
    this.id = id;
    this.path = path;
    this.type = type;
    this.tag = tag;
    this.datasetType = datasetType;
    this.containerType = containerType;
  }

  public static CatalogItem fromSourceConfig(SourceConfig sourceConfig) {
    return new CatalogItem(
      sourceConfig.getId().getId(),
      Lists.newArrayList(sourceConfig.getName()),
      String.valueOf(sourceConfig.getVersion()),
      CatalogItemType.CONTAINER,
      null,
      ContainerSubType.SOURCE
    );
  }

  public static CatalogItem fromHomeConfig(HomeConfig homeConfig) {
    return new CatalogItem(
      homeConfig.getId().getId(),
      Lists.newArrayList(HomeName.getUserHomePath(homeConfig.getOwner()).toString()),
      String.valueOf(homeConfig.getVersion()),
      CatalogItemType.CONTAINER,
      null,
      ContainerSubType.HOME
    );
  }

  public static CatalogItem fromSpaceConfig(SpaceConfig spaceConfig) {
    return new CatalogItem(
      spaceConfig.getId().getId(),
      Lists.newArrayList(spaceConfig.getName()),
      String.valueOf(spaceConfig.getVersion()),
      CatalogItemType.CONTAINER,
      null,
      ContainerSubType.SPACE
    );
  }

  public static CatalogItem fromDatasetConfig(DatasetConfig datasetConfig) {
    DatasetSubType datasetType = DatasetSubType.PROMOTED;

    if (datasetConfig.getType() == DatasetType.PHYSICAL_DATASET) {
      datasetType = DatasetSubType.DIRECT;
    } else if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
      datasetType = DatasetSubType.VIRTUAL;
    }

    return new CatalogItem(
      datasetConfig.getId().getId(),
      datasetConfig.getFullPathList(),
      String.valueOf(datasetConfig.getVersion()),
      CatalogItemType.DATASET,
      datasetType,
      null
    );
  }

  public static CatalogItem fromFolderConfig(FolderConfig folderConfig) {
    return new CatalogItem(
      folderConfig.getId().getId(),
      folderConfig.getFullPathList(),
      String.valueOf(folderConfig.getVersion()),
      CatalogItemType.CONTAINER,
      null,
      ContainerSubType.FOLDER
    );
  }

  public String getId() {
    return id;
  }

  public List<String> getPath() {
    return path;
  }

  public CatalogItemType getType() {
    return type;
  }

  public String getTag() {
    return tag;
  }

  public DatasetSubType getDatasetType() {
    return datasetType;
  }

  public ContainerSubType getContainerType() {
    return containerType;
  }

  public static Optional<CatalogItem> fromNamespaceContainer(NameSpaceContainer container) {
    Optional<CatalogItem> item = Optional.absent();

    switch (container.getType()) {
      case SOURCE: {
        item = Optional.of(CatalogItem.fromSourceConfig(container.getSource()));
        break;
      }

      case SPACE: {
        item = Optional.of(CatalogItem.fromSpaceConfig(container.getSpace()));
        break;
      }

      case DATASET: {
        item = Optional.of(CatalogItem.fromDatasetConfig(container.getDataset()));
        break;
      }

      case HOME: {
        item = Optional.of(CatalogItem.fromHomeConfig(container.getHome()));
        break;
      }

      case FOLDER: {
        item = Optional.of(CatalogItem.fromFolderConfig(container.getFolder()));
        break;
      }

      default:
        throw new UnsupportedOperationException(String.format("Catalog item of type [%s] can not be created", container.getType()));
    }

    return item;
  }

  @Override
  public String toString() {
    return "CatalogItem{" +
      "id='" + id + '\'' +
      ", path=" + path +
      ", type=" + type +
      ", tag='" + tag + '\'' +
      ", datasetType=" + datasetType +
      ", containerType=" + containerType +
      '}';
  }
}
