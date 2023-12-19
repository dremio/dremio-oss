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

import java.util.Optional;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.coordinator.ClusterElectionManager;
import com.dremio.service.coordinator.ClusterServiceSetManager;

/**
 * A wrapped modifiable scheduler service that hides the new clustered singleton under a feature flag
 * <p>
 * <strong>NOTE:</strong> for the new clustered singleton implementation, this wrapper uses the underlying singleton
 * scheduler service.
 * TODO: DX-68199; remove or refactor this when the old clustered singleton is entirely decommissioned.
 * </p>
 */
public class ModifiableWrappedSchedulerService implements ModifiableSchedulerService {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ModifiableWrappedSchedulerService.class);
  private final Provider<OptionManager> optionManagerProvider;
  private final PositiveLongValidator option;
  private final Provider<ModifiableSchedulerService> schedulerServiceProvider;
  private final String taskGroupName;
  private final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider;
  private final Provider<ClusterElectionManager> clusterElectionManagerProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> currentNodeProvider;
  private final ModifiableSchedulerService schedulerServiceInUse;
  private final int initialCapacity;
  private final boolean leaderlessEnabled;
  private final boolean isDistributedCoordinator;
  private volatile TaskGroupOptionListener taskGroupOptionListener;

  public ModifiableWrappedSchedulerService(int capacity,
                                           boolean isDistributedCoordinator,
                                           String taskGroupName,
                                           Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
                                           Provider<ClusterElectionManager> clusterElectionManagerProvider,
                                           Provider<CoordinationProtos.NodeEndpoint> currentNodeProvider,
                                           PositiveLongValidator option,
                                           Provider<OptionManager> optionManagerProvider,
                                           Provider<ModifiableSchedulerService> schedulerServiceProvider,
                                           boolean leaderlessEnabled) {
    this.optionManagerProvider = optionManagerProvider;
    this.option = option;
    this.taskGroupName = taskGroupName;
    this.clusterElectionManagerProvider = clusterElectionManagerProvider;
    this.clusterServiceSetManagerProvider = clusterServiceSetManagerProvider;
    this.currentNodeProvider = currentNodeProvider;
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.initialCapacity = capacity;
    this.leaderlessEnabled = leaderlessEnabled;
    this.isDistributedCoordinator = isDistributedCoordinator;
    // do this last
    this.schedulerServiceInUse = createModifiableSchedulerService();
  }

  @Override
  public void start() throws Exception {
    LOGGER.info("Starting Wrapped Scheduler Service");
    if (leaderlessEnabled) {
      LOGGER.info("Using leaderless Clustered singleton");
    } else {
      LOGGER.info("Using leader election based Clustered singleton");
      schedulerServiceInUse.start();
    }
    // register the task group option change listener
    taskGroupOptionListener = new TaskGroupOptionListener(schedulerServiceInUse, taskGroupName, option,
      optionManagerProvider);
    optionManagerProvider.get().addOptionChangeListener(taskGroupOptionListener);
    LOGGER.info("Wrapped Scheduler Service Started");
  }

  @Override
  public void addTaskGroup(ScheduleTaskGroup taskGroup) {
    // no op as all invocations are internal due to 1-to-1 mapping of this instance and task group and
    // the task group is directly invoked on the inner scheduler
  }

  @Override
  public void modifyTaskGroup(String groupName, ScheduleTaskGroup taskGroup) {
    // no op as all invocations are internal due to 1-to-1 mapping of this and task group
  }

  @Override
  public Cancellable schedule(Schedule schedule, Runnable task) {
    Schedule groupedSchedule = schedule;
    if (schedule.getTaskName() != null && schedule.getSingleShotType() == null) {
      groupedSchedule = Schedule.ClusteredSingletonBuilder.fromSchedule(schedule).taskGroup(taskGroupName).build();
    }
    return schedulerServiceInUse.schedule(groupedSchedule, task);
  }

  @Override
  public Optional<CoordinationProtos.NodeEndpoint> getCurrentTaskOwner(String taskName) {
    return schedulerServiceInUse.getCurrentTaskOwner(taskName);
  }

  @Override
  public void close() throws Exception {
    if (schedulerServiceInUse != null) {
      schedulerServiceInUse.close();
    }
  }

  private ModifiableSchedulerService createModifiableSchedulerService() {
    if (leaderlessEnabled) {
      LOGGER.info("Using leaderless Clustered singleton");
      final ModifiableSchedulerService service = schedulerServiceProvider.get();
      service.addTaskGroup(ScheduleTaskGroup.create(taskGroupName, initialCapacity));
      return service;
    } else {
      LOGGER.info("Using leader election based Clustered singleton");
      return new ModifiableLocalSchedulerService(
        initialCapacity,
        taskGroupName,
        clusterServiceSetManagerProvider,
        clusterElectionManagerProvider,
        currentNodeProvider,
        isDistributedCoordinator,
        option,
        optionManagerProvider);
    }
  }
}
