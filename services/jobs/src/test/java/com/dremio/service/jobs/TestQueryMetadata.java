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
package com.dremio.service.jobs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.List;

import org.junit.Test;

import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.google.common.collect.ImmutableList;

/**
 * Unit tests for QueryMetadata utility methods.
 */
public class TestQueryMetadata {
  @Test
  public void testGetSourcesPDS() {
    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.PHYSICAL_DATASET)
      .setFullPathList(ImmutableList.of("source_name", "table_name"));

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source_name");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesVDS() {
    final Origin origin = new Origin()
      .setColumnName("col1")
      .setDerived(false)
      .setTableList(ImmutableList.of("source_name", "schema_name", "table_name"));
    final FieldOrigin fieldOrigin = new FieldOrigin()
      .setName("col1")
      .setOriginsList(ImmutableList.of(origin));
    final VirtualDataset vds = new VirtualDataset()
      .setFieldOriginsList(ImmutableList.of(fieldOrigin));
    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("source_name", "table_name"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source_name");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesExtQueryWithParent() {
    final ParentDataset parentDataset = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source_name", "external_query"));
    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset));
    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source_name");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesExtQueryWithParentUpperCase() {
    final ParentDataset parentDataset = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source_name", "EXTERNAL_QUERY"));
    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset));
    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source_name");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesExtQueryWithParentMixedCase() {
    final ParentDataset parentDataset = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source_name", "EXteRNAl_qUEry"));
    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset));
    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source_name");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesExtQueryWithParents() {
    final ParentDataset parentDataset1 = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source1", "external_query"));
    final ParentDataset parentDataset2 = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source2", "external_query"));

    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset1, parentDataset2));

    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source1", "source2");

    assertThat(actual, equalTo(expected));
  }

  @Test
  public void testGetSourcesExtQueryWithParentsAndGrandParents() {
    final ParentDataset grandParentDataset1 = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source1", "external_query"));
    final ParentDataset grandParentDataset2 = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source2", "external_query"));

    final ParentDataset parentDataset = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("test_space", "test_table_1"));

    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset))
      .setGrandParentsList(ImmutableList.of(grandParentDataset1, grandParentDataset2));

    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table_2"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source1", "source2");

    assertThat(actual, equalTo(expected));
  }


  @Test
  public void testGetSourcesExtQueryWithPdsParents() {
    final ParentDataset parentDataset1 = new ParentDataset()
      .setDatasetPathList(ImmutableList.of("source1", "external_query"));
    final ParentDataset parentDataset2 = new ParentDataset()
      .setType(DatasetType.PHYSICAL_DATASET)
      .setDatasetPathList(ImmutableList.of("source2", "source2_schema", "source2_table"));

    final Origin origin = new Origin()
      .setColumnName("col1")
      .setDerived(false)
      .setTableList(ImmutableList.of("source2", "source2_schema", "source2_table"));
    final FieldOrigin fieldOrigin = new FieldOrigin()
      .setName("col1")
      .setOriginsList(ImmutableList.of(origin));

    final VirtualDataset vds = new VirtualDataset()
      .setParentsList(ImmutableList.of(parentDataset1, parentDataset2))
      .setFieldOriginsList(ImmutableList.of(fieldOrigin));

    final DatasetConfig datasetConfig = new DatasetConfig()
      .setType(DatasetType.VIRTUAL_DATASET)
      .setFullPathList(ImmutableList.of("test_space", "test_table"))
      .setVirtualDataset(vds);

    final List<String> actual = QueryMetadata.getSources(datasetConfig);
    final List<String> expected = ImmutableList.of("source1", "source2");

    assertThat(actual, equalTo(expected));
  }
}
