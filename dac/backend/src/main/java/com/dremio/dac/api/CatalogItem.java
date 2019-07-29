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
package com.dremio.dac.api;

import java.util.List;

import com.dremio.dac.model.spaces.HomeName;
import com.dremio.dac.proto.model.collaboration.CollaborationTag;
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
  private final CollaborationTag tags;
  private final CatalogItemStats stats;

  @JsonCreator
  protected CatalogItem(
    @JsonProperty("id") String id,
    @JsonProperty("path") List<String> path,
    @JsonProperty("tag") String tag,
    @JsonProperty("type") CatalogItemType type,
    @JsonProperty("datasetType") DatasetSubType datasetType,
    @JsonProperty("containerType") ContainerSubType containerType,
    @JsonProperty("tags") CollaborationTag tags,
    @JsonProperty("stats") CatalogItemStats stats) {
    this.id = id;
    this.path = path;
    this.type = type;
    this.tag = tag;
    this.datasetType = datasetType;
    this.containerType = containerType;
    this.tags = tags;
    this.stats = stats;
  }

  private static CatalogItem fromSourceConfig(SourceConfig sourceConfig, CollaborationTag tags) {
    return new Builder()
      .setId(sourceConfig.getId().getId())
      .setPath(Lists.newArrayList(sourceConfig.getName()))
      .setTag(String.valueOf(sourceConfig.getTag()))
      .setType(CatalogItemType.CONTAINER)
      .setContainerType(ContainerSubType.SOURCE)
      .setTags(tags)
      .build();
  }

  public static CatalogItem fromSourceConfig(SourceConfig sourceConfig) {
    return fromSourceConfig(sourceConfig, null);
  }

  public static CatalogItem fromHomeConfig(HomeConfig homeConfig) {
    return new Builder()
      .setId(homeConfig.getId().getId())
      .setPath(Lists.newArrayList(HomeName.getUserHomePath(homeConfig.getOwner()).toString()))
      .setTag(String.valueOf(homeConfig.getTag()))
      .setType(CatalogItemType.CONTAINER)
      .setContainerType(ContainerSubType.HOME)
      .build();
  }

  private static CatalogItem fromSpaceConfig(SpaceConfig spaceConfig, CollaborationTag tags) {
    return new Builder()
      .setId(spaceConfig.getId().getId())
      .setPath(Lists.newArrayList(spaceConfig.getName()))
      .setTag(String.valueOf(spaceConfig.getTag()))
      .setType(CatalogItemType.CONTAINER)
      .setContainerType(ContainerSubType.SPACE)
      .setTags(tags)
      .build();
  }

  public static CatalogItem fromSpaceConfig(SpaceConfig spaceConfig) {
    return fromSpaceConfig(spaceConfig, null);
  }

  public static CatalogItem fromDatasetConfig(DatasetConfig datasetConfig, CollaborationTag tags) {
    DatasetSubType datasetType = DatasetSubType.PROMOTED;

    if (datasetConfig.getType() == DatasetType.PHYSICAL_DATASET) {
      datasetType = DatasetSubType.DIRECT;
    } else if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
      datasetType = DatasetSubType.VIRTUAL;
    }

    return new Builder()
      .setId(datasetConfig.getId().getId())
      .setPath(Lists.newArrayList(datasetConfig.getFullPathList()))
      .setTag(String.valueOf(datasetConfig.getTag()))
      .setType(CatalogItemType.DATASET)
      .setDatasetType(datasetType)
      .setTags(tags)
      .build();
  }

  public static CatalogItem fromFolderConfig(FolderConfig folderConfig) {
    return new Builder()
      .setId(folderConfig.getId().getId())
      .setPath(Lists.newArrayList(folderConfig.getFullPathList()))
      .setTag(String.valueOf(folderConfig.getTag()))
      .setType(CatalogItemType.CONTAINER)
      .setContainerType(ContainerSubType.FOLDER)
      .build();
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

  public CollaborationTag getTags() {
    return tags;
  }

  public CatalogItemStats getStats() {
    return stats;
  }

  public static Optional<CatalogItem> fromNamespaceContainer(NameSpaceContainer container) {
    return fromNamespaceContainer(container, null);
  }

  public static Optional<CatalogItem> fromNamespaceContainer(NameSpaceContainer container, CollaborationTag tags) {
    Optional<CatalogItem> item = Optional.absent();

    switch (container.getType()) {
      case SOURCE: {
        item = Optional.of(CatalogItem.fromSourceConfig(container.getSource(), tags));
        break;
      }

      case SPACE: {
        item = Optional.of(CatalogItem.fromSpaceConfig(container.getSpace(), tags));
        break;
      }

      case DATASET: {
        item = Optional.of(CatalogItem.fromDatasetConfig(container.getDataset(), tags));
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

  /**
   * CatalogItem builder
   */
  public static class Builder {
    private String id;
    private List<String> path;
    private String tag;
    private CatalogItem.CatalogItemType type;
    private CatalogItem.DatasetSubType datasetType;
    private CatalogItem.ContainerSubType containerType;
    private CollaborationTag tags;
    private Integer datasetCount;
    private boolean datasetCountBounded;

    public Builder() {

    }

    public Builder(final CatalogItem item) {
      final CatalogItemStats stats = item.getStats();
      this
        .setId(item.getId())
        .setPath(item.getPath())
        .setTag(item.getTag())
        .setType(item.getType())
        .setDatasetType(item.getDatasetType())
        .setContainerType(item.getContainerType())
        .setTags(item.getTags());

      if (stats != null) {
        this
          .setDatasetCount(stats.getDatasetCount())
          .setDatasetCountBounded(stats.isDatasetCountBounded());
      }
    }

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setPath(List<String> path) {
      this.path = path;
      return this;
    }

    public Builder setTag(String tag) {
      this.tag = tag;
      return this;
    }

    public Builder setType(CatalogItem.CatalogItemType type) {
      this.type = type;
      return this;
    }

    public Builder setDatasetType(CatalogItem.DatasetSubType datasetType) {
      this.datasetType = datasetType;
      return this;
    }

    public Builder setContainerType(CatalogItem.ContainerSubType containerType) {
      this.containerType = containerType;
      return this;
    }

    public Builder setTags(CollaborationTag tags) {
      this.tags = tags;
      return this;
    }

    public Builder setDatasetCount(int datasetCount) {
      this.datasetCount = datasetCount;
      return this;
    }

    public Builder setDatasetCountBounded(boolean datasetCountBounded) {
      this.datasetCountBounded = datasetCountBounded;
      return this;
    }

    public CatalogItem build() {
      return new CatalogItem(id, path, tag, type, datasetType, containerType, tags,
        getStats());
    }

    public String getId() {
      return id;
    }

    public List<String> getPath() {
      return path;
    }

    public String getTag() {
      return tag;
    }

    public CatalogItemType getType() {
      return type;
    }

    public DatasetSubType getDatasetType() {
      return datasetType;
    }

    public ContainerSubType getContainerType() {
      return containerType;
    }

    public CollaborationTag getTags() {
      return tags;
    }

    public CatalogItemStats getStats() {
      if (datasetCount == null) {
        return null;
      }
      return new CatalogItemStats(datasetCount, datasetCountBounded);
    }
  }
}
