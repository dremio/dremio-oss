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
package com.dremio.exec.planner;

import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeByAttr;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeByMajor;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;

public class PlanFragmentStats {
  private IntSummaryStatistics combinedSize;
  private Map<Integer, IntSummaryStatistics> sizeByMajorSpecific;
  private Map<Integer, IntSummaryStatistics> sizeByMinorSpecific;

  private Map<String, Integer> sizeByMinorAttr;
  private Map<String, Integer> sizeBySharedAttr;
  private boolean updatedAttrSizes = false;

  public PlanFragmentStats() {
    sizeByMajorSpecific = new HashMap<>();
    sizeByMinorSpecific = new HashMap<>();
    sizeByMinorAttr = new HashMap<>();
    sizeBySharedAttr = new HashMap<>();
    combinedSize = new IntSummaryStatistics();
  }

  void addFragmentSize(boolean isMajorSpecific, int majorId, int size) {
    Map<Integer, IntSummaryStatistics> map =
        isMajorSpecific ? sizeByMajorSpecific : sizeByMinorSpecific;

    IntSummaryStatistics phaseStats = map.get(majorId);
    if (phaseStats == null) {
      phaseStats = new IntSummaryStatistics();
      map.put(majorId, phaseStats);
    }
    phaseStats.accept(size);
  }

  public void add(NodeEndpoint endPoint, InitializeFragments initializeFragments) {
    // Update combined size (can be different for each end-point).
    combinedSize.accept(initializeFragments.getSerializedSize());

    // Update per-major stats.
    PlanFragmentSet set = initializeFragments.getFragmentSet();
    for (PlanFragmentMajor major : set.getMajorList()) {
      int majorId = major.getHandle().getMajorFragmentId();
      addFragmentSize(true, majorId, major.getSerializedSize());

      int minorSize = 0;
      for (PlanFragmentMinor minor : set.getMinorList()) {
        if (minor.getMajorFragmentId() == majorId) {
          minorSize += minor.getSerializedSize();
        }
      }
      addFragmentSize(false, majorId, minorSize);
    }

    // update attribute sizes, group-by key. Do this only once.
    if (!updatedAttrSizes) {
      for (PlanFragmentMinor minor : set.getMinorList()) {
        for (MinorAttr attr : minor.getAttrsList()) {
          int attrSize = attr.getSerializedSize();

          // update hash table (indexed by attr name).
          int curSize = sizeByMinorAttr.getOrDefault(attr.getName(), 0);
          sizeByMinorAttr.put(attr.getName(), curSize + attrSize);
        }
      }

      // shared attributes.
      for (MinorAttr attr : set.getAttrList()) {
        sizeBySharedAttr.put(attr.getName(), attr.getSerializedSize());
      }
      updatedAttrSizes = true;
    }
  }

  public FragmentRpcSizeStats getSummary() {
    FragmentRpcSizeStats.Builder stats = FragmentRpcSizeStats.newBuilder();

    stats.setSizePerNode((int) combinedSize.getAverage());

    for (Entry<Integer, IntSummaryStatistics> entry : sizeByMajorSpecific.entrySet()) {
      IntSummaryStatistics majorStats = entry.getValue();
      IntSummaryStatistics minorStats = sizeByMinorSpecific.get(entry.getKey());

      stats.addFragments(
          FragmentRpcSizeByMajor.newBuilder()
              .setMajorId(entry.getKey())
              .setMajorPortionSize((int) majorStats.getAverage())
              .setMinorPortionSize((int) minorStats.getAverage())
              .build());
    }

    for (Entry<String, Integer> entry : sizeByMinorAttr.entrySet()) {
      stats.addMinorSpecificAttrs(
          FragmentRpcSizeByAttr.newBuilder()
              .setName(entry.getKey())
              .setSize(entry.getValue())
              .build());
    }
    for (Entry<String, Integer> entry : sizeBySharedAttr.entrySet()) {
      stats.addSharedAttrs(
          FragmentRpcSizeByAttr.newBuilder()
              .setName(entry.getKey())
              .setSize(entry.getValue())
              .build());
    }
    return stats.build();
  }
}
