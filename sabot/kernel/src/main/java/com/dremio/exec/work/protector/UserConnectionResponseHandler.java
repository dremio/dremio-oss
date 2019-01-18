/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.work.protector;

import com.dremio.common.exceptions.ErrorCompatibility;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.BaseRpcOutcomeListener;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.sabot.rpc.user.UserRPCServer.UserClientConnection;

public class UserConnectionResponseHandler implements UserResponseHandler {

  private final UserClientConnection client;

  public UserConnectionResponseHandler(UserClientConnection client) {
    super();
    this.client = client;
  }

  @Override
  public void completed(UserResult result) {
    client.sendResult(new BaseRpcOutcomeListener<Ack>(), ErrorCompatibility.convertIfNecessary(result.toQueryResult()));
  }

  @Override
  public void sendData(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch result) {
    client.sendData(outcomeListener, result);
  }

}
