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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.dremio.common.exceptions.UserException;

public class TestDeltaSnapshotListProcessor {

  @Test(expected = UserException.class)
  public void testMissingVersionInBetween() {
    List<DeltaLogSnapshot> list = generate(Arrays.asList(4, 12, 20), Arrays.asList(), 30);
    DeltaSnapshotListProcessor processor = new DeltaSnapshotListProcessor();

    try {
      processor.findValidSnapshots(list);
    }
    catch (Exception e) {
      assertTrue(e instanceof UserException);
      assertEquals(e.getMessage(), "Missing version file 20");
      throw e;
    }
  }

  @Test
  public void hasSingleCheckPoint() {
    List<DeltaLogSnapshot> list = generate(Arrays.asList(), Arrays.asList(20), 30);
    DeltaSnapshotListProcessor processor = new DeltaSnapshotListProcessor();

    List<DeltaLogSnapshot> finalSnaphosts = processor.findValidSnapshots(list);
    List<Long> actualSnapshotVersions = finalSnaphosts.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(actualSnapshotVersions, Arrays.asList(30L, 29L, 28L, 27L, 26L, 25L, 24L, 23L, 22L, 21L, 20L));
  }

  @Test
  public void emptySnapshotList() {
    List<DeltaLogSnapshot> list = Arrays.asList();
    DeltaSnapshotListProcessor processor = new DeltaSnapshotListProcessor();

    assertThatThrownBy(() -> processor.findValidSnapshots(list))
      .isInstanceOf(IllegalStateException.class)
      .hasMessage("Commit log is empty");
  }

  @Test
  public void multipleCheckpointsInBatch() {
    List<DeltaLogSnapshot> list = generate(Arrays.asList(), Arrays.asList(20, 26), 30);
    DeltaSnapshotListProcessor processor = new DeltaSnapshotListProcessor();

    List<DeltaLogSnapshot> finalSnaphosts = processor.findValidSnapshots(list);

    List<Long> actualSnapshotVersions = finalSnaphosts.stream().map(x -> x.getVersionId()).collect(Collectors.toList());

    assertEquals(actualSnapshotVersions,Arrays.asList(30L, 29L, 28L, 27L, 26L));
  }

  @Test(expected = IllegalStateException.class)
  public void missingVersionZero() {
    List<DeltaLogSnapshot> list = generate(Arrays.asList(0, 1, 2, 3, 4, 5), Arrays.asList(), 30);
    //Since we don't have a checkpoint file then we should have commit json 0. In all other cases we should have
    //atleast one checkpoint file present

    DeltaSnapshotListProcessor processor = new DeltaSnapshotListProcessor();
    try {
      processor.findValidSnapshots(list);
    }
    catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
      assertEquals(e.getMessage(), "Commit Json for version 0 not found while reading metadata");
      throw e;
    }
  }

  private List<DeltaLogSnapshot> generate(List<Integer> removeSnapshots, List<Integer> checkpointSnaphosts, Integer total) {
    ArrayList<DeltaLogSnapshot> snapshots = new ArrayList<>();

    for(int i = 0; i <= total; i++) {
      if(!removeSnapshots.contains(i)) {
        boolean isCheckpoint = checkpointSnaphosts.contains(i);
        DeltaLogSnapshot s = new DeltaLogSnapshot("UNKNOWN", 0, 0, 0, 0, 0, isCheckpoint);
        s.setVersionId(i);
        snapshots.add(s);
      }
    }

    return snapshots;
  }
}
