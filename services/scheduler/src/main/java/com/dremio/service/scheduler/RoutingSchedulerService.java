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

import static com.dremio.service.coordinator.ClusterCoordinator.Role.MASTER;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.ClusterServiceSetManager;
import java.util.Optional;
import javax.inject.Provider;

/**
 * An implementation of scheduler service that routes the schedule request either to the Local
 * scheduler or the clustered singleton scheduler.
 */
public class RoutingSchedulerService implements ModifiableSchedulerService {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(RoutingSchedulerService.class);
  private final Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider;
  private final SchedulerService localSchedulerService;
  private final ModifiableSchedulerService distributedSchedulerService;
  private volatile boolean started = false;

  public RoutingSchedulerService(
      Provider<ClusterServiceSetManager> clusterServiceSetManagerProvider,
      SchedulerService localSchedulerService,
      ModifiableSchedulerService distributedSchedulerService) {
    this.clusterServiceSetManagerProvider = clusterServiceSetManagerProvider;
    this.localSchedulerService = localSchedulerService;
    this.distributedSchedulerService = distributedSchedulerService;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(localSchedulerService, distributedSchedulerService);
    LOGGER.info("Routing Scheduler Service Stopped");
  }

  @Override
  public void start() throws Exception {
    if (!started) {
      localSchedulerService.start();
      if (distributedSchedulerService != null) {
        distributedSchedulerService.start();
      }
      started = true;
    }
    LOGGER.info("Routing Scheduler Service Started");
  }

  @Override
  public Cancellable schedule(Schedule schedule, Runnable task) {
    if (distributedSchedulerService == null || !schedule.isDistributedSingleton()) {
      return localSchedulerService.schedule(schedule, task);
    }
    return distributedSchedulerService.schedule(schedule, task);
  }

  @Override
  public Optional<CoordinationProtos.NodeEndpoint> getCurrentTaskOwner(String taskName) {
    if (distributedSchedulerService != null) {
      // we are a clustered singleton; the assumption here is that only distributed schedules asks
      // for the current
      // task owner; local schedules always has the local endpoint as the task leader which should
      // be inferred by the
      // caller when they get an empty optional.
      return distributedSchedulerService.getCurrentTaskOwner(taskName);
    } else {
      // if we are master less, there is only a single master. Thus return the same master endpoint
      // always for all tasks
      return Optional.ofNullable(
              clusterServiceSetManagerProvider.get().getServiceSet(MASTER).getAvailableEndpoints())
          .flatMap(nodeEndpoints -> nodeEndpoints.stream().findFirst());
    }
  }

  @Override
  public boolean isRollingUpgradeInProgress(String taskName) {
    return distributedSchedulerService != null
        && distributedSchedulerService.isRollingUpgradeInProgress(taskName);
  }

  @Override
  public void addTaskGroup(ScheduleTaskGroup taskGroup) {
    if (distributedSchedulerService != null) {
      distributedSchedulerService.addTaskGroup(taskGroup);
    }
  }

  @Override
  public void modifyTaskGroup(String groupName, ScheduleTaskGroup taskGroup) {
    if (distributedSchedulerService != null) {
      distributedSchedulerService.addTaskGroup(taskGroup);
    }
  }
}
