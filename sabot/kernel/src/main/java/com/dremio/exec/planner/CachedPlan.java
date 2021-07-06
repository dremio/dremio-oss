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

import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.physical.Prel;

public class CachedPlan {
  private final String queryText;
  private Prel prel;
  private int esitimatedSize;   //estimated size in byte
  private int useCount;
  private long creationTime;
  private SubstitutionInfo substitutionInfo;

  private CachedPlan(String query, Prel prel, int useCount, int esitimatedSize) {
    this.queryText = query;
    this.prel = prel;
    this.useCount = useCount;
    this.esitimatedSize = esitimatedSize;
    this.creationTime = System.currentTimeMillis();
  }

  public static CachedPlan createCachedPlan(String query, Prel prel, int esitimatedSize) {
    return new CachedPlan(query, prel, 0, esitimatedSize);
  }

  public Prel getPrel() {
    return prel;
  }

  public void setPrel(Prel prel) {
    this.prel = prel;
  }

  public void setUseCount(int useCount) {
    this.useCount = useCount;
  }

  public SubstitutionInfo getSubstitutionInfo() {
    return substitutionInfo;
  }

  public void setSubstitutionInfo(SubstitutionInfo info) {
    substitutionInfo = info;
  }

  public void updateUseCount() {
    this.useCount += 1;
  }

  public int getUseCount() {
    return useCount;
  }

  public int getEsitimatedSize() {
    return esitimatedSize;
  }

  public void setEsitimatedSize(int esitimatedSize) {
    this.esitimatedSize = esitimatedSize;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
