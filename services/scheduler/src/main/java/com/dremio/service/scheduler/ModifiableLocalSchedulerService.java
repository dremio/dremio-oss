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

import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;

/**
 * A LocalScheduleService in which the corePoolSize and maxPoolSize of the underlying
 * threadPoolExecutor is dynamically modified based on changes to option value.
 *
 * CorePoolSize and MaxPoolSize are always kept equal.
 */
public class ModifiableLocalSchedulerService extends LocalSchedulerService implements ModifiableSchedulerService {
  public ModifiableLocalSchedulerService(int corePoolSize, String threadNamePrefix,
                                         PositiveLongValidator option, OptionManager optionManager) {
    super(corePoolSize, threadNamePrefix);
    new ModifiableThreadPoolExecutor(getExecutorService(), option, optionManager);
  }
}

