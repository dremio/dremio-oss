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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.rpc.RpcException;
import com.dremio.resource.exception.ResourceAllocationException;

/** Handles the life-cycle of query during the execution phases (post logical planning). */
interface QueryTracker extends AutoCloseable {

  /**
   * Allocate resources required for the query (eg. slot in distributed queue).
   *
   * @throws ExecutionSetupException
   * @throws ResourceAllocationException
   */
  void allocateResources() throws ExecutionSetupException, ResourceAllocationException;

  /** Interrupts allocation, provided we are in an interruptible stage within allocation. */
  void interruptAllocation();

  /**
   * Execution planning include parallelization of the query fragments.
   *
   * @throws ExecutionSetupException
   */
  void planExecution() throws ExecutionSetupException;

  /**
   * Propagate the fragments to the executors.
   *
   * @throws ExecutionSetupException
   */
  void startFragments() throws ExecutionSetupException;

  /**
   * Handle completion of all fragments on given node.
   *
   * @param completion node completion.
   */
  void nodeCompleted(NodeQueryCompletion completion) throws RpcException;

  /**
   * Handle screen completion.
   *
   * @param completion screen completion.
   */
  void screenCompleted(NodeQueryScreenCompletion completion);

  /**
   * Handle first error reported by an executor node.
   *
   * @param firstError error
   */
  void nodeMarkFirstError(NodeQueryFirstError firstError);

  /** Cancel the fragments of a query. */
  void cancel();
}
