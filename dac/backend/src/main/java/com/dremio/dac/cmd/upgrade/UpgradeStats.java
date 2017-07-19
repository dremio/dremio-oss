/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade;

/**
 * Holds various stats updated by {@link UpgradeTask}
 */
class UpgradeStats {
  private int numLayouts;
  private int numMaterializedLayouts;
  private int numMaterializations;
  private int numAccelerations;

  void layoutUpdated() {
    numLayouts++;
  }

  void materializedLayoutUpdated() {
    numMaterializedLayouts++;
  }

  void materializationUpdated() {
    numMaterializations++;
  }

  void accelerationUpdated() {
    numAccelerations++;
  }

  @Override
  public String toString() {
    return String.format("Updated:\n\t%d accelerations" +
        "\n\t%d layouts" +
        "\n\t%d materialized layouts" +
        "\n\t%d materializations",
      numAccelerations, numLayouts, numMaterializedLayouts, numMaterializations);
  }
}
