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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.proto.EntityId;
import org.junit.Test;

/** Test for split id */
public class TestPartitionChunkId {

  @Test
  public void testIdFromConfig() throws Exception {
    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setId(new EntityId().setId("ds1"))
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L));

    PartitionChunkId split1 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s1").build(), 0L);
    PartitionChunkId split2 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s2").build(), 0L);
    PartitionChunkId split3 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s3").build(), 0L);

    assertEquals("ds1_0_s1", split1.getSplitId());
    assertEquals("ds1_0_s2", split2.getSplitId());
    assertEquals("ds1_0_s3", split3.getSplitId());
  }

  @Test
  public void testIdWithUnderscoreFromConfig() throws Exception {
    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setId(new EntityId().setId("ds1_test"))
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L));

    PartitionChunkId split1 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s1").build(), 0L);
    PartitionChunkId split2 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s2").build(), 0L);
    PartitionChunkId split3 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s3").build(), 0L);

    assertEquals("ds1%5Ftest_0_s1", split1.getSplitId());
    assertEquals("ds1%5Ftest_0_s2", split2.getSplitId());
    assertEquals("ds1%5Ftest_0_s3", split3.getSplitId());
  }

  @Test
  public void testIdWithPercentageFromConfig() throws Exception {
    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setId(new EntityId().setId("ds1%test"))
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L));

    PartitionChunkId split1 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s1").build(), 0L);
    PartitionChunkId split2 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s2").build(), 0L);
    PartitionChunkId split3 =
        PartitionChunkId.of(
            datasetConfig, PartitionChunk.newBuilder().setSplitKey("s3").build(), 0L);

    assertEquals("ds1%25test_0_s1", split1.getSplitId());
    assertEquals("ds1%25test_0_s2", split2.getSplitId());
    assertEquals("ds1%25test_0_s3", split3.getSplitId());
  }

  @Test
  public void testIdFromString() throws Exception {
    PartitionChunkId split1 = PartitionChunkId.of("ds1_1_s1");
    PartitionChunkId split2 = PartitionChunkId.of("ds2_2_s2");
    PartitionChunkId split3 = PartitionChunkId.of("ds3_3_s3");
    PartitionChunkId split4 = PartitionChunkId.of("ds4%5Ftest_4_s4");
    PartitionChunkId split5 = PartitionChunkId.of("ds5%25test_5_s5");

    assertEquals("ds1", split1.getDatasetId());
    assertEquals("ds2", split2.getDatasetId());
    assertEquals("ds3", split3.getDatasetId());
    assertEquals("ds4_test", split4.getDatasetId());
    assertEquals("ds5%test", split5.getDatasetId());
  }

  @Test
  public void testUnsafeIdFromString() throws Exception {
    PartitionChunkId split1 = PartitionChunkId.of("ds1_test_1_s1");

    assertEquals("ds1", split1.getDatasetId());
    assertEquals(Long.MIN_VALUE, split1.getSplitVersion());
  }

  @Test
  public void testInvalidIdFromString() throws Exception {
    try {
      PartitionChunkId split = PartitionChunkId.of("ds1_1");
      fail("ds1_1 is an invalid dataset split id");
    } catch (IllegalArgumentException e) {
    }

    try {
      PartitionChunkId split = PartitionChunkId.of("ds2_2_");
      fail("ds2_2_ is an invalid dataset split id");
    } catch (IllegalArgumentException e) {
    }
  }
}
