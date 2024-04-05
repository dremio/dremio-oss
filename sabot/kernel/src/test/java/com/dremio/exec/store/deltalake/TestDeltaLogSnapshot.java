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
package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.BaseTestQuery;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Tests for {@link DeltaLogSnapshot} */
public class TestDeltaLogSnapshot extends BaseTestQuery {

  private DeltaLogSnapshot parseSnapshot(String fileName, long version) throws IOException {
    DeltaLogSnapshot snapshot = TestDeltaLogCommitJsonReader.parseCommitJson(fileName);
    snapshot.setVersionId(version);
    return snapshot;
  }

  private DeltaLogSnapshot createSnapshot(boolean isCheckpoint, long version) {
    DeltaLogSnapshot snapshot =
        new DeltaLogSnapshot(
            DeltaConstants.OPERATION_WRITE, 1, 1, 1, 1, System.currentTimeMillis(), isCheckpoint);
    snapshot.setVersionId(version);
    return snapshot;
  }

  private DeltaLogSnapshot createSnapshot(
      boolean isCheckpoint, long version, String schema, List<String> partitionColumns) {
    DeltaLogSnapshot snapshot = createSnapshot(isCheckpoint, version);
    snapshot.setSchema(schema, partitionColumns, null);
    return snapshot;
  }

  @Test
  public void testMerge() throws IOException {
    DeltaLogSnapshot snapshot0 = parseSnapshot("/deltalake/test2_init.json", 0);
    DeltaLogSnapshot snapshot1 = parseSnapshot("/deltalake/test1_3.json", 1);
    DeltaLogSnapshot snapshot2 = parseSnapshot("/deltalake/test1_4.json", 2);

    DeltaLogSnapshot snapshot00 = snapshot0.clone();
    snapshot00.merge(snapshot1);
    snapshot00.merge(snapshot2);

    assertEquals(DeltaConstants.OPERATION_COMBINED, snapshot00.getOperationType());
    assertEquals(
        (snapshot0.getNetFilesAdded()
            + snapshot1.getNetFilesAdded()
            + snapshot2.getNetFilesAdded()),
        snapshot00.getNetFilesAdded());
    assertEquals(
        snapshot0.getNetOutputRows() + snapshot1.getNetOutputRows() + snapshot2.getNetOutputRows(),
        snapshot00.getNetOutputRows());
    assertEquals(
        snapshot0.getNetBytesAdded() + snapshot1.getNetBytesAdded() + snapshot2.getNetBytesAdded(),
        snapshot00.getNetBytesAdded());
    assertEquals(snapshot2.getSchema(), snapshot00.getSchema());
    assertEquals(snapshot2.getTimestamp(), snapshot00.getTimestamp());
    assertEquals(snapshot2.getPartitionColumns(), snapshot00.getPartitionColumns());

    // Different permutations of the merge
    DeltaLogSnapshot snapshot10 = snapshot1.clone();
    DeltaLogSnapshot snapshot20 = snapshot2.clone();
    snapshot20.merge(snapshot0);
    snapshot10.merge(snapshot20);
    assertEquals(snapshot00, snapshot10);
  }

  @Test
  public void testCompare() throws IOException {
    DeltaLogSnapshot snapshot0 = parseSnapshot("/deltalake/test2_init.json", 0);
    DeltaLogSnapshot snapshot1 = parseSnapshot("/deltalake/test1_3.json", 1);
    DeltaLogSnapshot snapshot2 = parseSnapshot("/deltalake/test1_4.json", 2);

    assertTrue(snapshot0.compareTo(snapshot2) < 0);
    assertTrue(snapshot1.compareTo(snapshot2) < 0);
    assertTrue(snapshot1.compareTo(snapshot0) > 0);
    assertTrue(snapshot0.clone().compareTo(snapshot0) == 0);
  }

  @Test
  public void testMergeOnVersion() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11, "schema1", new ArrayList<>());
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10, "schema2", new ArrayList<>());

    snapshot1.merge(snapshot2);
    assertEquals("schema1", snapshot1.getSchema());
  }

  @Test(expected = IllegalStateException.class)
  public void testMergeRepartition() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11, "schema1", ImmutableList.of("colA"));
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10, "schema2", ImmutableList.of("colB"));

    snapshot1.merge(snapshot2);
  }

  @Test(expected = IllegalStateException.class)
  public void testMergePartitionIntroduced() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11, "schema1", ImmutableList.of("colA"));
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10, "schema2", new ArrayList<>());

    snapshot1.merge(snapshot2);
  }

  @Test
  public void testMergeRepartitionNoMetadata() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11, "schema1", ImmutableList.of("colA"));
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10);

    snapshot1.merge(snapshot2);
  }

  @Test
  public void testMergeRepartitionNoMetadataBothSides() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11);
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10);

    snapshot1.merge(snapshot2);
  }

  @Test
  public void testMergeWithPartitions() {
    DeltaLogSnapshot snapshot1 = createSnapshot(false, 11, "schema1", ImmutableList.of("colA"));
    DeltaLogSnapshot snapshot2 = createSnapshot(true, 10, "schema2", ImmutableList.of("colA"));

    snapshot1.merge(snapshot2);
  }
}
