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
package com.dremio.exec.maestro;

import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.proto.UserBitShared.FragmentRpcSizeStats;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.work.QueryWorkUnit;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.resource.ResourceSchedulingDecisionInfo;

public interface MaestroObserver {

  /**
   * Called to report the wait in the command pool.
   * May be called multiple times during a query lifetime, as often as the query's tasks are put into the command pool
   */
  void commandPoolWait(long waitInMillis);


  /**
   * Generic ability to record extra information in a job.
   * @param name The name of the extra info. This can be thought of as a list rather than set and calls with the same name will all be recorded.
   * @param bytes The data to persist.
   */
  void recordExtraInfo(String name, byte[] bytes);

  /**
   * The planning and parallelization phase of the query is completed.
   *
   * An {@link ExecutionPlan execution plan} is provided to observer.
   */
  void planCompleted(ExecutionPlan plan);

  /**
   * The execution of the query started.
   * @param profile The initial query profile for the query.
   */
  void execStarted(QueryProfile profile);


  /**
   * Executor nodes were selected for the query
   */
  void executorsSelected(long millisTaken, int idealNumFragments, int idealNumNodes, int numExecutors, String detailsText);

  /**
   * Parallelization planning started
   */
  void planParallelStart();

  /**
   * The decisions made for parallelizations and fragments were completed.
   * @param planningSet
   */
  void planParallelized(PlanningSet planningSet);

  /**
   * Time taken to assign fragments to nodes.
   * @param millisTaken time in milliseconds
   */
  void planAssignmentTime(long millisTaken);

  /**
   * Time taken to generate fragments.
   * @param millisTaken time in milliseconds
   */
  void planGenerationTime(long millisTaken);

  /**
   * The decisions for distribution of work are completed.
   * @param unit The distribution decided for each node.
   */
  void plansDistributionComplete(QueryWorkUnit unit);

  /**
   * Number of records processed
   * @param recordCount records processed
   */
  void recordsProcessed(long recordCount);

  /**
   * Time taken for sending start fragment rpcs to all nodes.
   * @param millisTaken
   */
  void fragmentsStarted(long millisTaken, FragmentRpcSizeStats stats);

  /**
   * Time taken for sending activate fragment rpcs to all nodes.
   * @param millisTaken
   */
  void fragmentsActivated(long millisTaken);

  /**
   * Failed to activate fragment.
   * @param ex
   */
  void activateFragmentFailed(Exception ex);

  /**
   * ResourceScheduling related information
   * @param resourceSchedulingDecisionInfo
   */
  void resourcesScheduled(ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo);
}
