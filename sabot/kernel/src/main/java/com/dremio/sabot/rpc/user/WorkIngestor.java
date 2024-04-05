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

import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcException;
import io.netty.buffer.ByteBuf;

/**
 * Relied on by UserRpcServer. Abstracts away how individual rpc types are handled. Work may be
 * performed synch or asynch.
 */
interface WorkIngestor {

  /**
   * Determines what should work be done to service rpcType.
   *
   * @param connection
   * @param rpcType - type of rpc (e.g. Cancel Query, Resume Query)
   * @param pBody - Request body
   * @param dBody
   * @param responseSender - Used to send results or failures back to client
   * @throws RpcException - If unable to parse pbody
   */
  void feedWork(
      UserRPCServer.UserClientConnectionImpl connection,
      int rpcType,
      byte[] pBody,
      ByteBuf dBody,
      ResponseSender responseSender)
      throws RpcException;
}
