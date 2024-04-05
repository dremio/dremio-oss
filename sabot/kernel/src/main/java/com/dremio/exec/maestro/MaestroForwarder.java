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

import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.services.jobresults.common.JobResultsRequestWrapper;

/** forwards the request to target maestro server */
public interface MaestroForwarder extends AutoCloseable {

  void screenCompleted(NodeQueryScreenCompletion completion);

  void nodeQueryCompleted(NodeQueryCompletion completion);

  void nodeQueryMarkFirstError(NodeQueryFirstError completion);

  void dataArrived(JobResultsRequest jobResultsRequest, ResponseSender sender);

  void dataArrived(JobResultsRequestWrapper jobResultsRequestWrapper, ResponseSender sender);

  void resultsCompleted(String queryId);

  void resultsError(String queryId, Throwable exception);
}
