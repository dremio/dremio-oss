/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.spill;

import com.google.common.annotations.VisibleForTesting;

/**
 * Default implementation of {@link SpillServiceOptions}.
 * Also carries the default values of the various parameters
 * Its instantiation should only be used in tests, as it's impossible to set the options returned by this class
 */
public class DefaultSpillServiceOptions implements SpillServiceOptions {
  public static final boolean ENABLE_HEALTH_CHECK = true;
  public static final long MIN_DISK_SPACE_BYTES = 1024 * 1024 * 1024;
  public static final double MIN_DISK_SPACE_PCT = 1.0;
  public static final long HEALTH_CHECK_INTERVAL = 60 * 1000;
  public static final long SPILL_SWEEP_INTERVAL = 60 * 60 * 1000;             // spill sweep once an hour
  public static final long SPILL_SWEEP_THRESHOLD = 7L * 24 * 60 * 60 * 1000;  // remove spills older than one week

  @VisibleForTesting
  public DefaultSpillServiceOptions() {}

  @Override
  public boolean enableHealthCheck() {
    return ENABLE_HEALTH_CHECK;
  }

  @Override
  public long minDiskSpace() {
    return MIN_DISK_SPACE_BYTES;
  }

  @Override
  public double minDiskSpacePercentage() {
    return MIN_DISK_SPACE_PCT;
  }

  @Override
  public long healthCheckInterval() {
    return HEALTH_CHECK_INTERVAL;
  }

  @Override
  public long spillSweepInterval() {
    return SPILL_SWEEP_INTERVAL;
  }

  @Override
  public long spillSweepThreshold() {
    return SPILL_SWEEP_THRESHOLD;
  }
}
