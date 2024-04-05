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
package com.dremio.exec.store.sys.udf;

import com.dremio.exec.proto.FunctionRPC;
import com.dremio.exec.rpc.FutureBitCommand;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.services.fabric.ProxyConnection;

/** To get RequestFunctionInfo */
public class RequestFunctionInfo
    extends FutureBitCommand<FunctionRPC.FunctionInfoResp, ProxyConnection> {
  private final FunctionRPC.FunctionInfoReq FunctionInfoRequest;

  public RequestFunctionInfo(FunctionRPC.FunctionInfoReq FunctionInfoRequest) {
    super();
    this.FunctionInfoRequest = FunctionInfoRequest;
  }

  @Override
  public void doRpcCall(
      RpcOutcomeListener<FunctionRPC.FunctionInfoResp> outcomeListener,
      ProxyConnection connection) {
    connection.send(
        outcomeListener,
        FunctionRPC.RpcType.REQ_FUNCTION_INFO,
        FunctionInfoRequest,
        FunctionRPC.FunctionInfoResp.class);
  }
}
