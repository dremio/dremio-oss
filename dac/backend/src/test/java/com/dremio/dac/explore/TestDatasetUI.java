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
package com.dremio.dac.explore;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetType;

public class TestDatasetUI {
  private final List<String> homeDatasetFullPath = Arrays.asList("@dremio", "table1");
  private final List<String> spaceDatasetFullPath = Arrays.asList("view1");
  private final List<String> sourceDatasetFullPath = Arrays.asList("other", "table1");
  private final List<String> versionedPhysicalDatasetFullPath = Arrays.asList("versioned", "table1");
  private final List<String> versionedVirtualDatasetFullPath = Arrays.asList("versioned", "view1");

  private final String branchName = "main";
  private final DatasetVersion datasetVersion = DatasetVersion.MAX_VERSION;
  private final TableVersionContext versionContext =
      TableVersionContext.of(VersionContext.ofBranch(branchName));
  private final VersionedDatasetId versionedPhysicalDatasetId =
      VersionedDatasetId.newBuilder()
          .setTableKey(versionedPhysicalDatasetFullPath)
          .setContentId("5befad6b-9d77-4e36-a26c-a4b0c4eb0d08")
          .setTableVersionContext(versionContext)
          .build();
  private final VersionedDatasetId versionedVirtualDatasetId =
      VersionedDatasetId.newBuilder()
          .setTableKey(versionedVirtualDatasetFullPath)
          .setContentId("5befad6b-9d77-4e36-a26c-a4b0c4eb0d09")
          .setTableVersionContext(versionContext)
          .build();

  @Test
  public void testCreateLinksForVersionedPhysicalDataset() throws Exception {
    final Map<String, String> linksMap =
        DatasetUI.createLinks(
          versionedPhysicalDatasetFullPath,
          versionedPhysicalDatasetFullPath,
            datasetVersion,
            false,
            versionedPhysicalDatasetId.asString(),
            DatasetType.PHYSICAL_DATASET);

    assertThat(linksMap.get("edit"))
        .isEqualTo("/source/versioned/table1?mode=edit&version=7fffffffffffffff");
    assertThat(linksMap.get("self")).isEqualTo("/source/versioned/table1?version=7fffffffffffffff");
  }

  @Test
  public void testCreateLinksForVersionedVirtualDataset() throws Exception {
    final Map<String, String> linksMap =
        DatasetUI.createLinks(
          versionedVirtualDatasetFullPath,
          versionedVirtualDatasetFullPath,
            datasetVersion,
            false,
            versionedVirtualDatasetId.asString(),
            DatasetType.VIRTUAL_DATASET);

    assertThat(linksMap.get("edit"))
        .isEqualTo("/source/versioned/view1?mode=edit&version=7fffffffffffffff");
    assertThat(linksMap.get("self")).isEqualTo("/source/versioned/view1?version=7fffffffffffffff");
  }

  @Test
  public void testCreateLinksForHomePhysicalDataset() throws Exception {
    final Map<String, String> linksMap =
        DatasetUI.createLinks(
            homeDatasetFullPath,
            homeDatasetFullPath,
            datasetVersion,
            true,
            null,
            DatasetType.PHYSICAL_DATASET);

    assertThat(linksMap.get("edit"))
        .isEqualTo("/home/%40dremio/table1?mode=edit&version=7fffffffffffffff");
    assertThat(linksMap.get("self")).isEqualTo("/home/%40dremio/table1?version=7fffffffffffffff");
  }

  @Test
  public void testCreateLinksForSourcePhysicalDataset() throws Exception {
    final Map<String, String> linksMap =
        DatasetUI.createLinks(
            sourceDatasetFullPath,
            sourceDatasetFullPath,
            datasetVersion,
            true,
            null,
            DatasetType.PHYSICAL_DATASET);

    assertThat(linksMap.get("edit"))
        .isEqualTo("/source/other/table1?mode=edit&version=7fffffffffffffff");
    assertThat(linksMap.get("self")).isEqualTo("/source/other/table1?version=7fffffffffffffff");
  }

  @Test
  public void testCreateLinksForSpaceVirtualDataset() throws Exception {
    final Map<String, String> linksMap =
        DatasetUI.createLinks(
            spaceDatasetFullPath,
            spaceDatasetFullPath,
            datasetVersion,
            false,
            null,
            DatasetType.VIRTUAL_DATASET);

    assertThat(linksMap.get("edit")).isEqualTo("/space/view1/?mode=edit&version=7fffffffffffffff");
    assertThat(linksMap.get("self")).isEqualTo("/space/view1/?version=7fffffffffffffff");
  }
}
