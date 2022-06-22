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

import java.util.LinkedList;
import java.util.function.Function;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.join.vhash.spill.JoinSetupParams;
import com.dremio.sabot.op.join.vhash.spill.YieldingRunnable;
import com.dremio.sabot.op.join.vhash.spill.partition.Partition;

/**
 * Recursive Re-player for join spilling.
 *
 * 1. Picks the first entry off the replay list, and replay both build & probe.
 * 2. As part of (1), more replay entries can get generated and appended to the replay list if there is spilling
 *    (so, it's recursive).
 * 3. Delete the just processed entry from the replay list. Back to step (1).
 */
public class JoinRecursiveReplayer implements YieldingRunnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JoinRecursiveReplayer.class);
  private final LinkedList<JoinReplayEntry> replayList;
  private final Function<JoinReplayEntry, JoinReplayer> joinReplayerFactory;
  private JoinReplayer currentReplayer;

  public JoinRecursiveReplayer(JoinSetupParams setupParams, Partition partition, VectorContainer outgoing, int targetOutputSize) {
    this.replayList = setupParams.getReplayEntries();
    this.joinReplayerFactory = (entry) -> new JoinReplayer(entry, setupParams, partition, outgoing, targetOutputSize);
  }

  @Override
  public int run() throws Exception {
    if (currentReplayer != null) {
      int ret = currentReplayer.run();

      // if finished, close the replayer.
      if (currentReplayer.isFinished()) {
        logger.trace("replay entry finished");
        // this step can trigger more entries to be appended to the replayList
        currentReplayer.close();
        currentReplayer = null;
      }
      return ret;
    }

    if (!replayList.isEmpty()) {
      logger.trace("switching to new replay entry");
      JoinReplayEntry entry = replayList.removeFirst();
      currentReplayer = joinReplayerFactory.apply(entry);
    }
    return 0;
  }

  @Override
  public boolean isFinished() {
    return currentReplayer == null && replayList.isEmpty();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(currentReplayer);
    currentReplayer = null;
  }
}
