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
package com.dremio.service.nessie.maintenance;

import org.projectnessie.versioned.Hash;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.service.nessie.EmbeddedRepoPurgeParams;

/**
 * Reports Embedded Nessie maintenance operation progress to the admin log.
 */
public final class ProgressReporter extends EmbeddedRepoPurgeParams.ProgressConsumer {
  private final int reportCycle;
  private int numCommits;
  private int numKeyListEntitiesDeleted;

  public ProgressReporter(int reportCycle) {
    this.reportCycle = reportCycle;
  }

  @Override
  public void onCommitProcessed(Hash hash) {
    numCommits++;
    onEvent();
  }

  @Override
  public void onKeyListEntityDeleted(Hash hash) {
    numKeyListEntitiesDeleted++;
    onEvent();
  }

  private void onEvent() {
    if (reportCycle <= 0) {
      return;
    }

    if ((numCommits + numKeyListEntitiesDeleted) % reportCycle == 0) {
      AdminLogger.log("Processed {} commits. Deleted {} obsolete key list entities",
        numCommits,
        numKeyListEntitiesDeleted);
    }
  }
}
