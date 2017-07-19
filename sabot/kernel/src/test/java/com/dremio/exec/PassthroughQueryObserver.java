/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec;

import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.BaseRpcOutcomeListener;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnection;

public class PassthroughQueryObserver extends AbstractAttemptObserver {

  private UserClientConnection connection;

  public PassthroughQueryObserver(UserClientConnection connection) {
    super();
    this.connection = connection;
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    // this should replace the queryId in result with the externalId, but this
    // is only used for tests
    // so we'll just assume this will work fine as long we run one attempt per
    // query
    connection.sendData(outcomeListener, result);
  }


  @Override
  public void attemptCompletion(UserResult result) {
    connection.sendResult(new BaseRpcOutcomeListener<Ack>(), result.toQueryResult());
  }

}
