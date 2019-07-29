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

package com.dremio.connector.sample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.google.common.collect.Iterators;

/**
 *Test class for SampleSourceMetadata
 */
public class SampleSourceMetadataTest {
  private SampleSourceMetadata metadataConnector;

  @Before
  public void init() {
    metadataConnector = new SampleSourceMetadata();
  }

  @Test
  public void testAddSingleDataset() {
    List<String> pathToFile = Arrays.asList("a", "b");

    metadataConnector.addSingleDataset(pathToFile);
    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 1);
  }

  @Test
  public void testAddSingleDatasetAsList() {
    List<String> pathToFile = Arrays.asList("a", "b", "c");

    List<List<String>> allPathsToFile = new ArrayList<>();
    allPathsToFile.add(pathToFile);

    metadataConnector.addManyDatasets(allPathsToFile);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 1);
  }

  @Test
  public void testAddListOfDatasets() {
    List<List<String>> allPaths = new ArrayList<>();
    allPaths.add(Arrays.asList("a", "b"));
    allPaths.add(Arrays.asList("c", "d"));

    metadataConnector.addManyDatasets(allPaths);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);

  }

  @Test
  public void testExpectedDatasetEntityPath() {
    List<String> datasetPath = Arrays.asList("a", "b");
    metadataConnector.addSingleDataset(datasetPath);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 1);

    assertEquals(metadataConnector.getAllEntityPaths().get(0), new EntityPath(datasetPath));
  }

  @Test
  public void testBulkExpectedDatasetEntityPath() {
    List<String> pathA = Arrays.asList("a", "b");
    List<String> pathB = Arrays.asList("c", "d");

    metadataConnector
      .addSingleDataset(pathA);

    metadataConnector
      .addSingleDataset(pathB);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);

    assertEquals(metadataConnector.getAllEntityPaths().get(0).getComponents(), pathA);
    assertEquals(metadataConnector.getAllEntityPaths().get(1).getComponents(), pathB);
  }

  @Test
  public void testListDatasetHandles() {
    GetDatasetOption option = mock(GetDatasetOption.class);

    List<String> listA = Arrays.asList("a", "b");
    List<String> listB = Arrays.asList("c", "d");

    EntityPath pathA = new EntityPath(listA);
    EntityPath pathB = new EntityPath(listB);

    metadataConnector
      .addSingleDataset(listA);

    metadataConnector
      .addSingleDataset(listB);

    DatasetHandle handleA = metadataConnector.getAllDatasetHandles(option).get(0);
    DatasetHandle handleB = metadataConnector.getAllDatasetHandles(option).get(1);
    assertEquals(pathA, metadataConnector.getEntityPathFromHandle(handleA));
    assertEquals(pathB, metadataConnector.getEntityPathFromHandle(handleB));
  }

  @Test
  public void testGetDatasetHandle() {
    GetDatasetOption getDatasetOptions = mock(GetDatasetOption.class);

    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addSingleDataset(pathA);

    metadataConnector.addSingleDataset(pathB);

    assertNotNull(metadataConnector.getDatasetHandle(pathA, getDatasetOptions));
  }

  @Test
  public void testAddAPartitionChunk() {
    GetDatasetOption option = mock(GetDatasetOption.class);

    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addSingleDataset(pathA);

    metadataConnector.addSingleDataset(pathB);

    DatasetHandle handleA = metadataConnector.getAllDatasetHandles(option).get(0);

    assertNotNull(handleA);

    metadataConnector.addPartitionChunk(handleA);

    assertEquals(Iterators.size(metadataConnector.getPartitionChunks(handleA)), 1);
  }

  @Test
  public void testAddNPartitionChunks() {
    GetDatasetOption option = mock(GetDatasetOption.class);

    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addSingleDataset(pathA);

    metadataConnector.addSingleDataset(pathB);

    DatasetHandle handleA = metadataConnector.getAllDatasetHandles(option).get(0);

    assertNotNull(handleA);

    metadataConnector.addPartitionChunk(handleA);
    metadataConnector.addNPartitionChunks(handleA, 5);

    assertEquals(Iterators.size(metadataConnector.getPartitionChunks(handleA)), 6);
  }

  @Test
  public void testAddADataset() {
    EntityPath path = new EntityPath(Arrays.asList("a", "b"));

    metadataConnector.addSingleDataset(path);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 1);
  }

  @Test
  public void testSingleEntityPath() {
    EntityPath path = new EntityPath(Arrays.asList("a", "b"));

    metadataConnector.addSingleDataset(path);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 1);
    assertEquals(metadataConnector.getAllEntityPaths().get(0), path);
  }

  @Test
  public void testAddSequentialDatasets() {
    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addSingleDataset(pathA);

    metadataConnector.addSingleDataset(pathB);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);
  }

  @Test
  public void testMultipleEntityPaths() {
    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addSingleDataset(pathA);

    metadataConnector.addSingleDataset(pathB);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);
    assertEquals(metadataConnector.getAllEntityPaths().get(0), pathA);
    assertEquals(metadataConnector.getAllEntityPaths().get(1), pathB);
  }

  @Test
  public void testAddBulkDatasets() {
    EntityPath pathA = new EntityPath(Arrays.asList("a", "b"));
    EntityPath pathB = new EntityPath(Arrays.asList("c", "d"));

    metadataConnector.addManyDatasets(Arrays.asList(pathA, pathB), 0);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);
  }

  @Test
  public void testAddNDatasets() {
    metadataConnector.addNDatasets(3, 0, 0);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 3);
  }

  @Test
  public void testAddNAndAnother() {
    metadataConnector
      .addNDatasets(3, 0, 0);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 3);

    EntityPath path = new EntityPath(Arrays.asList("a", "b"));

    metadataConnector.addSingleDataset(path);

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 4);
  }

  @Test
  public void addDatasetsAtPaths() {
    List<String> path0 = Arrays.asList("a", "b");
    List<String> path1 = Arrays.asList("c", "d");

    metadataConnector.addManyDatasets(Arrays.asList(path0, path1));

    assertEquals(metadataConnector.getAllDatasetMetadata().size(), 2);
  }

  @Test
  public void testAddingDataset() {
    metadataConnector.addNDatasets(1, 1, 1);


    assertEquals(metadataConnector.getAllDatasetHandles(null).size(), 1);
  }

  @Test
  public void testAddingDatasetWithChunksAndSplits() {
    metadataConnector.addNDatasets(1, 1, 1);

    assertEquals(metadataConnector.getAllDatasetHandles(null).size(), 1);
    DatasetHandle datasetHandle = metadataConnector.getAllDatasetHandles(null).get(0);

    Iterator<? extends PartitionChunk> partitionChunkIterator = metadataConnector.getPartitionChunks(datasetHandle);
    assertEquals(Iterators.size(partitionChunkIterator), 1);

    while (partitionChunkIterator.hasNext()) {
      PartitionChunk partitionChunk = partitionChunkIterator.next();

      Iterator<? extends DatasetSplit> datasetSplitIterator = partitionChunk.getSplits().iterator();
      assertEquals(Iterators.size(datasetSplitIterator), 1);
    }
  }
}
