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
package com.dremio.service.namespace.dataset;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Objects;
import io.protostuff.ByteString;
import java.util.List;

/** Metadata of a dataset */
public final class DatasetMetadata {
  private EntityId id;
  private String name;
  private String owner;
  private DatasetType type;
  private List<String> fullPath;
  private VirtualDataset virtualDataset;
  private PhysicalDataset physicalDataset;
  private ReadDefinition readDefinition;
  private ByteString recordSchema;

  DatasetMetadata(
      EntityId id,
      String name,
      String owner,
      DatasetType type,
      List<String> fullPath,
      VirtualDataset virtualDataset,
      PhysicalDataset physicalDataset,
      ReadDefinition readDefinition,
      ByteString recordSchema) {
    this.id = id;
    this.name = name;
    this.owner = owner;
    this.type = type;
    this.fullPath = fullPath;
    this.virtualDataset = virtualDataset;
    this.physicalDataset = physicalDataset;
    this.readDefinition = readDefinition;
    this.recordSchema = recordSchema;
  }

  public static DatasetMetadata from(DatasetConfig datasetConfig) {
    return new DatasetMetadata(
        datasetConfig.getId(),
        datasetConfig.getName(),
        datasetConfig.getOwner(),
        datasetConfig.getType(),
        datasetConfig.getFullPathList(),
        datasetConfig.getVirtualDataset(),
        datasetConfig.getPhysicalDataset(),
        datasetConfig.getReadDefinition(),
        datasetConfig.getRecordSchema());
  }

  public EntityId getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public DatasetType getType() {
    return type;
  }

  public List<String> getFullPathList() {
    return fullPath;
  }

  public VirtualDataset getVirtualDataset() {
    return virtualDataset;
  }

  public PhysicalDataset getPhysicalDataset() {
    return physicalDataset;
  }

  public ReadDefinition getReadDefinition() {
    return readDefinition;
  }

  public ByteString getRecordSchema() {
    return recordSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatasetMetadata that = (DatasetMetadata) o;
    return Objects.equal(id, that.id)
        && Objects.equal(name, that.name)
        && Objects.equal(owner, that.owner)
        && type == that.type
        && Objects.equal(fullPath, that.fullPath)
        && Objects.equal(virtualDataset, that.virtualDataset)
        && Objects.equal(physicalDataset, that.physicalDataset)
        && Objects.equal(readDefinition, that.readDefinition)
        && Objects.equal(recordSchema, that.recordSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        id,
        name,
        owner,
        type,
        fullPath,
        virtualDataset,
        physicalDataset,
        readDefinition,
        recordSchema);
  }
}
