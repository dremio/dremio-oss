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

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.FunctionRPC;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.services.fabric.api.FabricCommandRunner;

/** To access FunctionService from executor(client) */
public class FunctionTunnel {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FunctionTunnel.class);

  private final CoordinationProtos.NodeEndpoint ep;
  private final FabricCommandRunner manager;

  public FunctionTunnel(CoordinationProtos.NodeEndpoint ep, FabricCommandRunner manager) {
    super();
    this.ep = ep;
    this.manager = manager;
  }

  public RpcFuture<FunctionRPC.FunctionInfoResp> requestFunctionInfos() {
    FunctionRPC.FunctionInfoReq FunctionInfoRequest =
        FunctionRPC.FunctionInfoReq.newBuilder().build();
    RequestFunctionInfo b = new RequestFunctionInfo(FunctionInfoRequest);
    manager.runCommand(b);
    return b.getFuture();
  }
}
