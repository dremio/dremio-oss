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

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.coordinator.ClusterElectionManager;
import com.dremio.service.coordinator.ClusterServiceSetManager;

/**
 * A LocalScheduleService in which the corePoolSize and maxPoolSize of the underlying
 * threadPoolExecutor is dynamically modified based on changes to option value.
 * CorePoolSize and MaxPoolSize are always kept equal.
 * TODO DX-68199; remove this altogether when the old clustered singleton gets decommissioned
 */
public class ModifiableLocalSchedulerService extends LocalSchedulerService implements ModifiableSchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ModifiableLocalSchedulerService.class);

  private final Provider<OptionManager> optionManagerProvider;
  private final PositiveLongValidator option;

  public ModifiableLocalSchedulerService(int corePoolSize, String threadNamePrefix,
                                         PositiveLongValidator option, Provider<OptionManager> optionManagerProvider) {
    super(corePoolSize, threadNamePrefix);
    this.optionManagerProvider = optionManagerProvider;
    this.option = option;
  }

  public ModifiableLocalSchedulerService(int corePoolSize,
                                         String threadNamePrefix,
                                         Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                                         Provider<ClusterElectionManager> clusterElectionManagerProvider,
                                         Provider<CoordinationProtos.NodeEndpoint> currentNode,
                                         boolean isDistributedCoordinator,
                                         PositiveLongValidator option,
                                         Provider<OptionManager> optionManagerProvider) {
    super(corePoolSize, threadNamePrefix, clusterServiceSetManagerProvider, clusterElectionManagerProvider, currentNode,
      isDistributedCoordinator);
    this.optionManagerProvider = optionManagerProvider;
    this.option = option;
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("ModifiableLocalSchedulerService is starting");
    super.start();
    final int poolSize = (int) optionManagerProvider.get().getOption(option);
    getExecutorService().setCorePoolSize(poolSize);
    getExecutorService().setMaximumPoolSize(poolSize);
    LOGGER.info("ModifiableLocalSchedulerService is up");
  }

  @Override
  public void addTaskGroup(ScheduleTaskGroup taskGroup) {
    // noop as the task group has a 1-to-1 dependency to this scheduler instance and there are
    // no external calls.
  }

  /**
   * Modifies a task group.
   * <p>
   * Called from the associated task group change listener {@code TaskGroupChangeListener}
   * </p>
   * @param groupName name of the group
   * @param taskGroup The modified group
   */
  @Override
  public void modifyTaskGroup(String groupName, ScheduleTaskGroup taskGroup) {
    final ThreadPoolExecutor threadPoolExecutor = getExecutorService();
    int newPoolSize = (int) optionManagerProvider.get().getOption(option);
    int currentPoolSize = threadPoolExecutor.getCorePoolSize();
    if (currentPoolSize == newPoolSize) {
      return;
    }
    LOGGER.info("Task group `{}` capacity modified from {} to {}", groupName, currentPoolSize, newPoolSize);
    // increase/decrease core pool and max pool size in correct order
    if (currentPoolSize > newPoolSize) {
      threadPoolExecutor.setCorePoolSize(newPoolSize);
      threadPoolExecutor.setMaximumPoolSize(newPoolSize);
    } else {
      threadPoolExecutor.setMaximumPoolSize(newPoolSize);
      threadPoolExecutor.setCorePoolSize(newPoolSize);
    }
    LOGGER.info("CorePoolSize and MaxPoolSize are updated from {} to {}", currentPoolSize, newPoolSize);
  }
}
