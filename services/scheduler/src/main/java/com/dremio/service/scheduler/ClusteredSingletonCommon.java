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

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.coordinator.LinearizableHierarchicalStore;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interface using which the various task manager classes within the scheduler package can pull
 * common <i>one time</i> information created by the {@code ClusteredSingletonTaskScheduler} that
 * never changes for all scheduled tasks.
 *
 * <p><strong>NOTE:</strong> This is an internal interface that is used only within the clustered
 * singleton and not exposed outside to other layers. The purpose of this interface is to pull
 * common config information across all tasks and provide it to the package private {@code Task*}
 * classes within the Clustered singleton module without exposing the clustered singleton main
 * implementation to them.
 *
 * <p><strong>NOTE:</strong>Keeping this as a package private abstract class rather than interface
 * to avoid breaking visibility rules for the {@code ClusteredSingletonTaskScheduler} as the {@code
 * ClusteredSingletonTaskScheduler} is a public class.
 *
 * <p><strong>NOTE:</strong>Only information that is common across all sub task-managers of the
 * clustered singleton service is exposed here.
 */
abstract class ClusteredSingletonCommon {
  static final Function<Set<CoordinationProtos.NodeEndpoint>, String> ENDPOINTS_AS_STRING =
      (endpoints) ->
          endpoints.stream()
              .map(e -> e.getAddress() + ":" + e.getFabricPort())
              .collect(Collectors.joining(" , "));

  static final Function<CoordinationProtos.NodeEndpoint, String> ENDPOINT_AS_STRING =
      (endpoint) -> endpoint.getAddress() + ":" + endpoint.getFabricPort();

  // Gets the name of the service
  abstract String getBaseServiceName();

  abstract String getFqServiceName();

  // Gets the service level done path for the given version of the service instance
  abstract String getVersionedDoneFqPath();

  // Gets the service level done path for all versions. Used by single shot schedules that do not do
  // anything
  // special on upgrades
  abstract String getUnVersionedDoneFqPath();

  // Gets the service level steal path. Common to all versions.
  abstract String getStealFqPath();

  // Gets the common pool used for scheduler specific tasks
  abstract CloseableSchedulerThreadPool getSchedulePool();

  // Gets a pool that queues up tasks to run sticky tasks
  abstract CloseableThreadPool getStickyPool();

  // Gets the opened task store
  abstract LinearizableHierarchicalStore getTaskStore();

  // Gets the task cancellation handler that consumes the cancelled task
  abstract String getServiceVersion();

  // gets the endpoint of this service instance
  abstract CoordinationProtos.NodeEndpoint getThisEndpoint();

  // maximum time to wait before completely cancelling a task
  abstract int getMaxWaitTimePostCancel();

  // whether the clustered singleton is still active and is NOT in the process of shutting down
  abstract boolean isActive();

  // Tells the recovery monitor to treat this instance as a zombie and thus ignore taking
  // recovery action.
  // for testing purposes only; allow tests to control how the recovery monitor behaves
  abstract boolean isZombie();

  // Tests may ask the recovery monitor to ignore reconnect requests. This allows the tests
  // to control when the system will act on ZK reconnects. This is needed because the current
  // ZK session expiration does not have capability to control when the reconnect event arrives
  // post session expiration.
  // for testing purposes only;
  abstract boolean shouldIgnoreReconnects();

  // Fully qualified root path for tracking weights. Non null only when weight based balancing
  // is enabled.
  abstract String getWeightFqPath();
}
