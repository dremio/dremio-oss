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
package com.dremio.sabot.exec.rpc;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecRPC.FragmentRecordBatch;
import com.dremio.exec.proto.ExecRPC.FragmentStreamComplete;
import com.dremio.exec.proto.ExecRPC.RpcType;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.RpcConfig;

public class ExecToExecConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecToExecConfig.class);

  public static RpcConfig getMapping(SabotConfig config) {
    return RpcConfig.newBuilder()
        .name("DATA")
        .timeout(config.getInt(ExecConstants.BIT_RPC_TIMEOUT))
        .add(RpcType.REQ_RECORD_BATCH, FragmentRecordBatch.class, RpcType.ACK, Ack.class)
        .add(RpcType.REQ_STREAM_COMPLETE, FragmentStreamComplete.class, RpcType.ACK, Ack.class)
        .build();
  }

  public static final Response OK = new Response(RpcType.ACK, Acks.OK);
  public static final Response FAIL = new Response(RpcType.ACK, Acks.FAIL);
}
