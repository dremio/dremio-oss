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
import com.dremio.exec.proto.UserBitShared.AccelerationProfile;
import java.util.concurrent.atomic.AtomicInteger;

public class CachedPlan {
  private final Prel prel;
  private final int estimatedSize; // estimated size in byte
  private AtomicInteger useCount;
  private final long creationTime;
  private AccelerationProfile accelerationProfile;

  private CachedPlan(Prel prel, int useCount, int estimatedSize) {
    this.prel = prel;
    this.useCount = new AtomicInteger(useCount);
    this.estimatedSize = estimatedSize;
    this.creationTime = System.currentTimeMillis();
  }

  public static CachedPlan createCachedPlan(Prel prel, int estimatedSize) {
    return new CachedPlan(prel, 0, estimatedSize);
  }

  public Prel getPrel() {
    return prel;
  }

  public AccelerationProfile getAccelerationProfile() {
    return accelerationProfile;
  }

  public void setAccelerationProfile(AccelerationProfile profile) {
    this.accelerationProfile = profile;
  }

  public int updateUseCount() {
    return this.useCount.incrementAndGet();
  }

  public int getUseCount() {
    return useCount.get();
  }

  public int getEstimatedSize() {
    return estimatedSize;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
