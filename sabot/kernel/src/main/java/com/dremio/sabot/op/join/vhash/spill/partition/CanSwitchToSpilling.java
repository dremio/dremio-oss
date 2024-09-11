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
package com.dremio.sabot.op.join.vhash.spill.partition;

import org.apache.arrow.memory.ArrowBuf;

public interface CanSwitchToSpilling {
  final class SwitchResult {
    private final boolean switchDone;
    private final Partition newPartition;

    SwitchResult(boolean switchDone, Partition newPartition) {
      this.switchDone = switchDone;
      this.newPartition = newPartition;
    }

    public boolean isSwitchDone() {
      return switchDone;
    }

    public Partition getNewPartition() {
      return newPartition;
    }
  }

  /**
   * Estimate of number of bytes that can be released by switching to spilling mode.
   *
   * @return bytes that can be released.
   */
  long estimateSpillableBytes();

  default void updateTableHashBuffer(ArrowBuf newBuf) {}

  /**
   * Switch to spilling
   *
   * @param spillAll spill all
   * @return result of the switch attempt
   */
  SwitchResult switchToSpilling(boolean spillAll);
}
