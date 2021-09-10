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

import com.dremio.exec.planner.physical.Prel;

public class CachedPlan {
  private final String queryText;
  private Prel prel;
  private String textPlan;
  private int estimatedSize;   //estimated size in byte
  private int useCount;
  private long creationTime;
  private CachedAccelDetails accelDetails;

  private CachedPlan(String query, Prel prel, String textPlan, int useCount, int estimatedSize) {
    this.queryText = query;
    this.prel = prel;
    this.textPlan = textPlan;
    this.useCount = useCount;
    this.estimatedSize = estimatedSize;
    this.creationTime = System.currentTimeMillis();
  }

  public static CachedPlan createCachedPlan(String query, Prel prel, String textPlan, int esitimatedSize) {
    return new CachedPlan(query, prel, textPlan, 0, esitimatedSize);
  }

  public Prel getPrel() {
    return prel;
  }

  public String getTextPlan() {
    return textPlan;
  }

  public void setAccelDetails(CachedAccelDetails accelDetails) {
    this.accelDetails = accelDetails;
  }

  public CachedAccelDetails getAccelDetails() {
    return accelDetails;
  }

  public void updateUseCount() {
    this.useCount += 1;
  }

  public int getUseCount() {
    return useCount;
  }

  public int getEstimatedSize() {
    return estimatedSize;
  }

  public void setEstimatedSize(int estimatedSize) {
    this.estimatedSize = estimatedSize;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
