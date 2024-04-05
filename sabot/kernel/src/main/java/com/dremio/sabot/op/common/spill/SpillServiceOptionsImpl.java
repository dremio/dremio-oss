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
package com.dremio.sabot.op.common.spill;

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.spill.SpillServiceOptions;
import javax.inject.Provider;

/** Provides options to the spill service */
public class SpillServiceOptionsImpl implements SpillServiceOptions {
  final Provider<OptionManager> optionManager;

  public SpillServiceOptionsImpl(final Provider<OptionManager> optionManager) {
    this.optionManager = optionManager;
  }

  private OptionManager options() {
    return optionManager.get();
  }

  @Override
  public boolean enableHealthCheck() {
    return options().getOption(ExecConstants.SPILL_ENABLE_HEALTH_CHECK);
  }

  @Override
  public long minDiskSpace() {
    return options().getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_BYTES);
  }

  @Override
  public double minDiskSpacePercentage() {
    return options().getOption(ExecConstants.SPILL_DISK_SPACE_LIMIT_PERCENTAGE);
  }

  @Override
  public long healthCheckInterval() {
    return options().getOption(ExecConstants.SPILL_DISK_SPACE_CHECK_INTERVAL);
  }

  @Override
  public long spillSweepInterval() {
    return options().getOption(ExecConstants.SPILL_SWEEP_INTERVAL);
  }

  @Override
  public long spillSweepThreshold() {
    return options().getOption(ExecConstants.SPILL_SWEEP_THRESHOLD);
  }
}
