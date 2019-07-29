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
package com.dremio.sabot.rpc.user;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.rpc.ConnectionThrottle;

public interface UserResultsListener {

  /**
   * QueryId is available. Called when a query is successfully submitted to the server.
   * @param queryId sent by the server along {@link com.dremio.sabot.rpc.Acks.OK Acks.OK}
   */
  void queryIdArrived(QueryId queryId);

  /**
   * The query has failed. Most likely called when the server returns a FAILED query state. Can also be called if
   * {@link #dataArrived(QueryDataBatch, ConnectionThrottle) dataArrived()} throws an exception
   * @param ex exception describing the cause of the failure
   */
  void submissionFailed(UserException ex);

  /**
   * A {@link com.dremio.exec.proto.beans.QueryData QueryData} message was received
   * @param result data batch received
   * @param throttle connection throttle
   */
  void dataArrived(QueryDataBatch result, ConnectionThrottle throttle);

  /**
   * The query has completed (successsful completion or cancellation). The listener will not receive any other
   * data or result message. Called when the server returns a terminal-non failing- state (COMPLETED or CANCELLED)
   * @param state
   */
  void queryCompleted(QueryState state);

}
