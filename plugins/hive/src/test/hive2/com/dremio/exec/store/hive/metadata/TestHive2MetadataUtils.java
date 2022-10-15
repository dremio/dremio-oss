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
package com.dremio.exec.store.hive.metadata;

import com.dremio.exec.store.TimedRunnable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHive2MetadataUtils {
  @Test
  public void testGetInputSplitSizesTimeoutCalculation() {
    List<InputSplit> inputSplitList = getInputSplitList(6, 10);
    Pair p1 = getTimeOutAndMaxDeltas(inputSplitList);
    inputSplitList = getInputSplitList(6, 61);
    Pair p2 = getTimeOutAndMaxDeltas(inputSplitList);
    assertTrue((long)p1.getLeft() < (long)p2.getLeft());
    assertTrue((long)p1.getRight() < (long)p2.getRight());

    inputSplitList = getInputSplitList(20, 20);
    p1 = getTimeOutAndMaxDeltas(inputSplitList);
    inputSplitList = getInputSplitList(2, 41);
    p2 = getTimeOutAndMaxDeltas(inputSplitList);
    assertTrue((long)p1.getLeft() < (long)p2.getLeft());
    assertTrue((long)p1.getRight() < (long)p2.getRight());

    inputSplitList = getNonOrcInputSplitList(12);
    p1 = getTimeOutAndMaxDeltas(inputSplitList);
    assertEquals(1, (long)p1.getLeft());
    assertEquals(2000l, (long)p1.getRight());
  }

  private Pair<Long,Long> getTimeOutAndMaxDeltas(List<InputSplit> inputSplitList) {
    List<TimedRunnable<Long>> jobs = new ArrayList<>();
    long maxDeltas = HiveMetadataUtils.populateSplitJobAndGetMaxDeltas(null, null, inputSplitList, jobs);
    long timeOut = HiveMetadataUtils.getMaxTimeoutPerCore(inputSplitList, maxDeltas);
    return Pair.of(maxDeltas, timeOut);
  }

  private List<InputSplit> getInputSplitList(int totalSplits, int totalDeltas) {
    int[] deltaSplitSize = new int[totalSplits];
    Random rand = new Random();
    for (int i = 0; i < totalDeltas; i++) {
      int splitNum = rand.nextInt(totalSplits);
      deltaSplitSize[splitNum]++;
    }

    List<InputSplit> inputSplits = new ArrayList<>();
    for (int i = 0; i < totalSplits; i++) {
      List<AcidInputFormat.DeltaMetaData> deltaMetaDataList = new ArrayList<>();
      for (int j = 0; j < deltaSplitSize[i]; j++) {
        AcidInputFormat.DeltaMetaData delta = new AcidInputFormat.DeltaMetaData();
        deltaMetaDataList.add(delta);
      }
      OrcSplit inputSplit = new OrcSplit(null,null,0,0, null, null, false, false,
        deltaMetaDataList,0,0);
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }

  private List<InputSplit> getNonOrcInputSplitList(int totalSplits) {
    List<InputSplit> inputSplits = new ArrayList<>();
    for (int i = 0; i < totalSplits; i++) {
      InputSplit inputSplit = new ParquetInputFormat.ParquetSplit();
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }
}
