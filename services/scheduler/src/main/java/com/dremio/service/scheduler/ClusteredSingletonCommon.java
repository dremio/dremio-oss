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

/**
 * Interface using which the various task manager classes within the scheduler package can pull common <i>one time</i>
 * information created by the {@code ClusteredSingletonTaskScheduler} that never changes for all scheduled tasks.
 * <p>
 * <strong>NOTE:</strong> This is an internal interface that is used only within the clustered singleton and not exposed
 * outside to other layers. The purpose of this interface is to pull common config information across all tasks and
 * provide it to the package private {@code  Task*} classes within the Clustered singleton module without exposing the
 * clustered singleton main implementation to them.
 * </p>
 * <p>
 * <strong>NOTE:</strong>Keeping this as a package private abstract class rather than interface to avoid breaking
 * visibility rules for the {@code ClusteredSingletonTaskScheduler} as the {@code ClusteredSingletonTaskScheduler}
 * is a public class.
 * </p>
 * <p>
 * <strong>NOTE:</strong>Only information that is common across all sub task-managers of the clustered singleton
 * service is exposed here.
 * </p>
 */
abstract class ClusteredSingletonCommon {
  // Gets the name of the service
  abstract String getBaseServiceName();
  abstract String getFqServiceName();
  // Gets the service level done path for the given version of the service instance
  abstract String getVersionedDoneFqPath();
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
  // whether the clustered singleton is still active and is NOT in the process of shutting down
  abstract boolean isActive();
}
