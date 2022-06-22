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
package com.dremio.sabot.op.join.vhash.spill;

import java.util.LinkedList;

import com.dremio.common.AutoCloseables;
import com.google.common.base.Preconditions;

/**
 * Wrapper over multiple memory releasers.
 */
public class MultiMemoryReleaser implements MemoryReleaser {
  private final LinkedList<MemoryReleaser> memoryReleasers = new LinkedList<>();

  @Override
  public int run() throws Exception {
    MemoryReleaser releaser = memoryReleasers.getFirst();
    releaser.run();
    if (releaser.isFinished()) {
      MemoryReleaser removed = memoryReleasers.removeFirst();
      Preconditions.checkState(removed == releaser);

      releaser.close();
    }
    return 0;
  }

  public void addReleaser(MemoryReleaser releaser) {
    memoryReleasers.addLast(releaser);
  }

  @Override
  public long getCurrentMemorySize() {
    long totalMemory = 0;
    for (MemoryReleaser releaser : memoryReleasers) {
      totalMemory += releaser.getCurrentMemorySize();
    }
    return totalMemory;
  }

  @Override
  public boolean isFinished() {
    return memoryReleasers.isEmpty();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(memoryReleasers);
    memoryReleasers.clear();
  }
}
