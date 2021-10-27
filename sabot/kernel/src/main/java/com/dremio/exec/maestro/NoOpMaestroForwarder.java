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

import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.service.jobresults.JobResultsRequest;

/**
 * NoOp implementation of MaestroForwarder
 */
public class NoOpMaestroForwarder implements MaestroForwarder {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NoOpMaestroForwarder.class);

  @Override
  public void screenCompleted(NodeQueryScreenCompletion completion) {
    logger.debug("screen completion message arrived post query termination, dropping. Query [{}] from node {}.",
      QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
  }

  @Override
  public void nodeQueryCompleted(NodeQueryCompletion completion) {
    logger.debug("A node query completion message arrived post query termination, dropping. Query [{}] from node {}.",
      QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
  }

  @Override
  public void nodeQueryMarkFirstError(NodeQueryFirstError error) {
    logger.debug("A node query error message arrived post query termination, dropping. Query [{}] from node {}.",
      QueryIdHelper.getQueryId(error.getHandle().getQueryId()), error.getEndpoint());
  }

  @Override
  public void dataArrived(JobResultsRequest request, ResponseSender sender) {
    logger.debug("User data arrived post query termination, dropping. Data was from QueryId: {}.",
      QueryIdHelper.getQueryId(request.getHeader().getQueryId()));
  }

  @Override
  public void resultsCompleted(String queryId) {
    logger.debug("No-op forwarder got results. Dropping it for query {}", queryId);
  }

  @Override
  public void resultsError(String queryId, Throwable exception) {
    logger.debug("No-op forwarder got results error. Dropping it for query {}", queryId);
  }
}
