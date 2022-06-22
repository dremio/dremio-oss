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

import java.util.List;

import com.dremio.sabot.op.sort.external.SpillManager.SpillFile;
import com.google.common.collect.ImmutableList;

/**
 * A set of join spill files that can be replayed in isolation to any other data/partitions.
 */
public class JoinReplayEntry {
  // list of build files
  private final List<SpillFile> buildFiles;
  // list of probe files
  private final List<SpillFile> probeFiles;

  JoinReplayEntry(List<SpillFile> buildFiles, List<SpillFile> probeFiles) {
    this.buildFiles = buildFiles;
    this.probeFiles = probeFiles;
  }

  public static JoinReplayEntry of(List<SpillFile> preBuildSpillFiles, SpillFile buildFile, SpillFile probeFile) {
    ImmutableList.Builder<SpillFile> buildFilesBuilder = new ImmutableList.Builder<>();
    buildFilesBuilder.addAll(preBuildSpillFiles);
    if (buildFile != null) {
      buildFilesBuilder.add(buildFile);
    }
    return new JoinReplayEntry(
      buildFilesBuilder.build(),
      probeFile == null ? ImmutableList.of() : ImmutableList.of(probeFile)
    );
  }

  List<SpillFile> getBuildFiles() {
    return this.buildFiles;
  }

  List<SpillFile> getProbeFiles() {
    return this.probeFiles;
  }
}
