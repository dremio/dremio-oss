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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.service.namespace.dataset.DatasetMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.Iterables;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Test class for TestDatasetMetadata */
public class TestDatasetMetadata {

  @Test
  public void testDatasetMetadataFromTheSameDatasetConfigShouldBeEqual() {
    String id = UUID.randomUUID().toString();
    List<String> path = List.of("space", "view");
    String owner = "test";
    Long createAt = 123456L;
    Long lastModified = 123457L;
    VirtualDataset virtualDataset = new VirtualDataset();
    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setId(new EntityId(id))
            .setName(Iterables.getLast(path))
            .setOwner(owner)
            .setType(DatasetType.VIRTUAL_DATASET)
            .setFullPathList(path)
            .setCreatedAt(createAt)
            .setLastModified(lastModified)
            .setVirtualDataset(virtualDataset);

    DatasetMetadata datasetMetadata1 = DatasetMetadata.from(datasetConfig);
    DatasetMetadata datasetMetadata2 = DatasetMetadata.from(datasetConfig);
    DatasetMetadata datasetMetadata3 = DatasetMetadata.from(datasetConfig);

    Set<DatasetMetadata> datasetMetadataSet = new HashSet<>();
    datasetMetadataSet.add(datasetMetadata1);
    datasetMetadataSet.add(datasetMetadata2);
    datasetMetadataSet.add(datasetMetadata3);
    assertEquals(1, datasetMetadataSet.size());
  }

  @Test
  public void testDatasetMetadataFromEqualDatasetConfigShouldBeEqualToo() {
    String id = UUID.randomUUID().toString();
    List<String> path = List.of("space", "view");
    String owner = "test";
    Long createAt = 123456L;
    Long lastModified = 123457L;
    VirtualDataset virtualDataset = new VirtualDataset();
    DatasetConfig datasetConfig1 =
        new DatasetConfig()
            .setId(new EntityId(id))
            .setName(Iterables.getLast(path))
            .setOwner(owner)
            .setType(DatasetType.VIRTUAL_DATASET)
            .setFullPathList(path)
            .setCreatedAt(createAt)
            .setLastModified(lastModified)
            .setVirtualDataset(virtualDataset);

    DatasetConfig datasetConfig2 =
        new DatasetConfig()
            .setId(new EntityId(id))
            .setName(Iterables.getLast(path))
            .setOwner(owner)
            .setType(DatasetType.VIRTUAL_DATASET)
            .setFullPathList(path)
            .setCreatedAt(createAt)
            .setLastModified(lastModified)
            .setVirtualDataset(virtualDataset);

    assertEquals(datasetConfig1, datasetConfig2);

    DatasetMetadata datasetMetadata1 = DatasetMetadata.from(datasetConfig1);
    DatasetMetadata datasetMetadata2 = DatasetMetadata.from(datasetConfig2);
    assertEquals(datasetMetadata1, datasetMetadata2);
  }
}
