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
package com.dremio.service.scheduler;

import javax.inject.Provider;

import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * Listen to changes to configured Option and accordingly
 * modify the associated task group
 */
public class TaskGroupOptionListener implements OptionChangeListener {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TaskGroupOptionListener.class);
  private final Provider<OptionManager> optionManager;
  private final PositiveLongValidator option;
  private final ModifiableSchedulerService schedulerService;
  private final String taskGroupName;

  public TaskGroupOptionListener(ModifiableSchedulerService scheduler, String taskGroupName,
                                 PositiveLongValidator option, Provider<OptionManager> optionManager) {
    this.schedulerService = scheduler;
    this.option = option;
    this.optionManager = optionManager;
    this.taskGroupName = taskGroupName;
  }

  @Override
  public synchronized void onChange() {
    final int capacity = (int) optionManager.get().getOption(option);
    LOGGER.debug("Option change request received, capacity = {} group = {}", capacity, taskGroupName);
    schedulerService.modifyTaskGroup(taskGroupName, ScheduleTaskGroup.create(taskGroupName, capacity));
  }

}
