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

import com.dremio.exec.store.hive.HiveSettings;

/**
 * Set of parameters controlling the process of estimating records in a hive table/partition
 */
public class StatsEstimationParameters {
  private final boolean useMetastoreStats;
  private final int listSizeEstimate;
  private final int varFieldSizeEstimate;
  private final HiveSettings hiveSettings;

  /**
   * @param useMetastoreStats    Whether to use stats in metastore or estimate based on filesize/filetype/record size
   * @param listSizeEstimate     Estimated number of elements in a list data type columns
   * @param varFieldSizeEstimate Estimated size of variable width columns
   * @param hiveSettings         hiveSettings to check additional settings where needed
   */
  public StatsEstimationParameters(final boolean useMetastoreStats, final int listSizeEstimate,
                                   final int varFieldSizeEstimate, final HiveSettings hiveSettings) {
    this.useMetastoreStats = useMetastoreStats;
    this.listSizeEstimate = listSizeEstimate;
    this.varFieldSizeEstimate = varFieldSizeEstimate;
    this.hiveSettings = hiveSettings;
  }

  public boolean useMetastoreStats() {
    return useMetastoreStats;
  }

  public int getListSizeEstimate() {
    return listSizeEstimate;
  }

  public int getVarFieldSizeEstimate() {
    return varFieldSizeEstimate;
  }

  public HiveSettings getHiveSettings() { return hiveSettings; }
}
