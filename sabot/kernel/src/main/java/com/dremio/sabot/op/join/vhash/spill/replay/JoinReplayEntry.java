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
package com.dremio.sabot.op.join.vhash.spill.replay;

import com.dremio.sabot.op.join.vhash.spill.io.SpillFileDescriptor;
import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** A set of join spill files that can be replayed in isolation to any other data/partitions. */
public class JoinReplayEntry {
  // list of build files
  private final List<SpillFileDescriptor> buildSpills;
  // list of probe files
  private final List<SpillFileDescriptor> probeSpills;

  JoinReplayEntry(List<SpillFileDescriptor> buildSpills, List<SpillFileDescriptor> probeSpills) {
    this.buildSpills = buildSpills;
    this.probeSpills = probeSpills;
  }

  public static JoinReplayEntry of(
      List<SpillFileDescriptor> preBuildSpills,
      SpillFileDescriptor buildSpill,
      SpillFileDescriptor probeSpill) {
    ImmutableList.Builder<SpillFileDescriptor> buildFilesBuilder = new ImmutableList.Builder<>();
    buildFilesBuilder.addAll(preBuildSpills);
    if (buildSpill != null) {
      buildFilesBuilder.add(buildSpill);
    }
    return new JoinReplayEntry(
        buildFilesBuilder.build(),
        probeSpill == null ? ImmutableList.of() : ImmutableList.of(probeSpill));
  }

  private long getCumulativeSize(List<SpillFileDescriptor> spills) throws IOException {
    long total = 0;
    for (SpillFileDescriptor desc : spills) {
      total += desc.getSizeInBytes();
    }
    return total;
  }

  private long getCumulativeNumRecords(List<SpillFileDescriptor> spills) throws IOException {
    long total = 0;
    for (SpillFileDescriptor desc : spills) {
      total += desc.getNumRecords();
    }
    return total;
  }

  public long getBuildSize() throws IOException {
    return getCumulativeSize(buildSpills);
  }

  public long getBuildNumRecords() throws IOException {
    return getCumulativeNumRecords(buildSpills);
  }

  public long getProbeSize() throws IOException {
    return getCumulativeSize(probeSpills);
  }

  public long getProbeNumRecords() throws IOException {
    return getCumulativeNumRecords(probeSpills);
  }

  List<SpillFile> getBuildFiles() {
    return buildSpills.stream().map(SpillFileDescriptor::getFile).collect(Collectors.toList());
  }

  List<SpillFile> getProbeFiles() {
    return probeSpills.stream().map(SpillFileDescriptor::getFile).collect(Collectors.toList());
  }
}
