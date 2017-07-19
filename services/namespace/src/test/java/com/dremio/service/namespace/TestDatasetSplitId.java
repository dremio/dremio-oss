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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;

/**
 * Test for split id
 */
public class TestDatasetSplitId {

  @Test
  public void testIdFromConfig() throws Exception {
    DatasetConfig datasetConfig = new DatasetConfig()
      .setId(new EntityId().setId("ds1"))
      .setReadDefinition(new ReadDefinition().setSplitVersion(0L));

    DatasetSplitId split1 = new DatasetSplitId(datasetConfig, new DatasetSplit().setSplitKey("s1"), 0L);
    DatasetSplitId split2 = new DatasetSplitId(datasetConfig, new DatasetSplit().setSplitKey("s2"), 0L);
    DatasetSplitId split3 = new DatasetSplitId(datasetConfig, new DatasetSplit().setSplitKey("s3"), 0L);

    assertEquals("ds1_0_s1", split1.getSpiltId());
    assertEquals("ds1_0_s2", split2.getSpiltId());
    assertEquals("ds1_0_s3", split3.getSpiltId());
  }

  @Test
  public void testIdFromString() throws Exception {
    DatasetSplitId split1 = new DatasetSplitId("ds1_1_s1");
    DatasetSplitId split2 = new DatasetSplitId("ds2_2_s2");
    DatasetSplitId split3 = new DatasetSplitId("ds3_3_s3");

    assertEquals("ds1", split1.getDatasetId());
    assertEquals("ds2", split2.getDatasetId());
    assertEquals("ds3", split3.getDatasetId());
  }

  @Test
  public void testInvalidIdFromString() throws Exception {
    try {
      DatasetSplitId split = new DatasetSplitId("ds1_1");
      fail("ds1_1 is an invalid dataset split id");
    } catch (IllegalArgumentException e) {
    }

    try {
      DatasetSplitId split = new DatasetSplitId("ds2_2_");
      fail("ds2_2_ is an invalid dataset split id");
    } catch (IllegalArgumentException e) {
    }
  }

}
