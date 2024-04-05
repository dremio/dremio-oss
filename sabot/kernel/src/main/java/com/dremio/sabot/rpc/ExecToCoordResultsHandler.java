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
package com.dremio.sabot.rpc;

import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.services.jobresults.common.JobResultsRequestWrapper;
import io.netty.buffer.ByteBuf;

/** Handler for result messages going from executor to coordinator. */
public interface ExecToCoordResultsHandler {

  void dataArrived(
      QueryData header, ByteBuf data, JobResultsRequest jobResultsRequest, ResponseSender sender)
      throws RpcException;

  // return true if result was forwarded
  boolean dataArrived(JobResultsRequestWrapper jobResultsRequestWrapper, ResponseSender sender)
      throws RpcException;
}
