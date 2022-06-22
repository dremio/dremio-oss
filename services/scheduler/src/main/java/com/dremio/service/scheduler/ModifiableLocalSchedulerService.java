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

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.coordinator.ClusterElectionManager;
import com.dremio.service.coordinator.ClusterServiceSetManager;

/**
 * A LocalScheduleService in which the corePoolSize and maxPoolSize of the underlying
 * threadPoolExecutor is dynamically modified based on changes to option value.
 *
 * CorePoolSize and MaxPoolSize are always kept equal.
 */
public class ModifiableLocalSchedulerService extends LocalSchedulerService implements ModifiableSchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ModifiableLocalSchedulerService.class);

  private final ModifiableThreadPoolExecutor threadPoolModifier;
  private final Provider<OptionManager> optionManagerProvider;
  private final PositiveLongValidator option;

  public ModifiableLocalSchedulerService(int corePoolSize, String threadNamePrefix,
                                         PositiveLongValidator option, Provider<OptionManager> optionManagerProvider) {
    super(corePoolSize, threadNamePrefix);
    this.optionManagerProvider = optionManagerProvider;
    this.option = option;
    this.threadPoolModifier = new ModifiableThreadPoolExecutor(getExecutorService(), option, optionManagerProvider);
  }

  public ModifiableLocalSchedulerService(int corePoolSize,
                                         String threadNamePrefix,
                                         Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                                         Provider<ClusterElectionManager> clusterElectionManagerProvider,
                                         Provider<CoordinationProtos.NodeEndpoint> currentNode,
                                         boolean assumeTaskLeadership,
                                         PositiveLongValidator option,
                                         Provider<OptionManager> optionManagerProvider) {
    super(corePoolSize, threadNamePrefix, clusterServiceSetManagerProvider, clusterElectionManagerProvider, currentNode, assumeTaskLeadership);
    this.optionManagerProvider = optionManagerProvider;
    this.option = option;
    this.threadPoolModifier = new ModifiableThreadPoolExecutor(getExecutorService(), option, optionManagerProvider);
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("ModifiableLocalSchedulerService is starting");
    super.start();
    threadPoolModifier.setInitialPoolSize((int) optionManagerProvider.get().getOption(option));
    optionManagerProvider.get().addOptionChangeListener(threadPoolModifier);
    LOGGER.info("ModifiableLocalSchedulerService is up");
  }
}
