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

import java.util.concurrent.ThreadPoolExecutor;

import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * Listen to changes to configured Option and accordingly
 * modify the corePoolSize and maxPoolSize of the provided threadPoolExecutor
 *
 * CorePoolSize and MaxPoolSize are always kept equal.
 */
public class ModifiableThreadPoolExecutor implements OptionChangeListener {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ModifiableThreadPoolExecutor.class);
  private int currentPoolSize;
  private OptionManager optionManager;
  private PositiveLongValidator option;
  private ThreadPoolExecutor threadPoolExecutor;

  public ModifiableThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor, PositiveLongValidator option, OptionManager optionManager) {
    this.threadPoolExecutor = threadPoolExecutor;
    this.option = option ;
    this.optionManager = optionManager;
    currentPoolSize = (int) optionManager.getOption(option);
    threadPoolExecutor.setCorePoolSize(currentPoolSize);
    threadPoolExecutor.setMaximumPoolSize(currentPoolSize);
    optionManager.addOptionChangeListener(this);
  }

  public synchronized void onChange() {
    int newPoolSize = (int) optionManager.getOption(option);
    if (currentPoolSize == newPoolSize) {
      return;
    }

    // increase/decrease core pool and max pool size in correct order
    if (currentPoolSize > newPoolSize) {
      threadPoolExecutor.setCorePoolSize(newPoolSize);
      threadPoolExecutor.setMaximumPoolSize(newPoolSize);
    } else if (currentPoolSize < newPoolSize) {
      threadPoolExecutor.setMaximumPoolSize(newPoolSize);
      threadPoolExecutor.setCorePoolSize(newPoolSize);
    }
    logger.info("CorePoolSize and MaxPoolSize are updated from {} to {}", currentPoolSize, newPoolSize);
    currentPoolSize = newPoolSize;
  }

}
