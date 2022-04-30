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
package com.dremio.service.autocomplete.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests for the CatalogReader
 */
public final class CatalogReaderTests {
  private static final HomeCatalogNode TEST_HOME_CATALOG_NODE = new HomeCatalogNode(
    "@dremio",
    ImmutableList.<CatalogNode>builder()
      .add(new SpaceCatalogNode(
        "space",
        ImmutableList.<CatalogNode>builder()
          .add(new FolderCatalogNode(
            "folder",
            ImmutableList.<CatalogNode>builder()
              .add(new FileCatalogNode("file"))
              .add(new SourceCatalogNode("source", ImmutableList.<CatalogNode>builder().add(new FileCatalogNode("source_file")).build()))
              .add(new DatasetCatalogNode("physical_dataset", DatasetCatalogNode.Type.Physical))
              .add(new DatasetCatalogNode("virtual_dataset", DatasetCatalogNode.Type.Virtual))
              .build()))
          .build()))
      .build());
  public static final MockCatalogReader MOCK_CATALOG_READER = new MockCatalogReader(TEST_HOME_CATALOG_NODE);

  @Test
  public void testPathing() {
    {
      List<String> emptyPath = new ArrayList<>(Arrays.asList());
      Optional<CatalogNode> nodeAtEmptyPath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(emptyPath);
      Assert.assertTrue(nodeAtEmptyPath.isPresent());
      Assert.assertTrue(nodeAtEmptyPath.get() instanceof HomeCatalogNode);
    }

    {
      List<String> spacePath = new ArrayList<>(Arrays.asList(
        "space"
      ));
      Optional<CatalogNode> nodeAtSpacePath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(spacePath);
      Assert.assertTrue(nodeAtSpacePath.isPresent());
      Assert.assertTrue(nodeAtSpacePath.get() instanceof SpaceCatalogNode);
    }

    {
      List<String> folderPath = new ArrayList<>(Arrays.asList(
        "space",
        "folder"
      ));
      Optional<CatalogNode> nodeAtFolderPath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(folderPath);
      Assert.assertTrue(nodeAtFolderPath.isPresent());
      Assert.assertTrue(nodeAtFolderPath.get() instanceof FolderCatalogNode);
    }

    {
      List<String> filePath = new ArrayList<>(Arrays.asList(
        "space",
        "folder",
        "file"
      ));
      Optional<CatalogNode> nodeAtFilePath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(filePath);
      Assert.assertTrue(nodeAtFilePath.isPresent());
      Assert.assertTrue(nodeAtFilePath.get() instanceof FileCatalogNode);
    }

    {
      List<String> sourcePath = new ArrayList<>(Arrays.asList(
        "space",
        "folder",
        "source"
      ));
      Optional<CatalogNode> nodeAtPath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(sourcePath);
      Assert.assertTrue(nodeAtPath.isPresent());
      Assert.assertTrue(nodeAtPath.get() instanceof SourceCatalogNode);
    }

    {
      List<String> virtualDatasetPath = new ArrayList<>(Arrays.asList(
        "space",
        "folder",
        "virtual_dataset"
      ));
      Optional<CatalogNode> nodeAtPath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(virtualDatasetPath);
      Assert.assertTrue(nodeAtPath.isPresent());
      Assert.assertTrue(nodeAtPath.get() instanceof DatasetCatalogNode);
      DatasetCatalogNode datasetCatalogNode = (DatasetCatalogNode) nodeAtPath.get();
      Assert.assertEquals(DatasetCatalogNode.Type.Virtual, datasetCatalogNode.getType());
    }

    {
      List<String> nonExistentPath = new ArrayList<>(Arrays.asList(
        "some",
        "path",
        "that",
        "doesn't",
        "exist"
      ));
      Optional<CatalogNode> nodeAtPath = MOCK_CATALOG_READER.tryGetCatalogNodeAtPath(nonExistentPath);
      Assert.assertFalse(nodeAtPath.isPresent());
    }
  }
}
