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
package com.dremio.services.fabric.api;

import com.dremio.exec.rpc.RpcCommand;
import com.dremio.services.fabric.ProxyConnection;
import com.google.protobuf.MessageLite;

/** Represents a way to run commands associated with a particular node-to-node connection. */
public interface FabricCommandRunner {

  /**
   * Run an asynchronous RpcCommand on the channel associated with this runner.
   *
   * @param cmd The command to be run.
   */
  public <R extends MessageLite, C extends RpcCommand<R, ProxyConnection>> void runCommand(C cmd);
}
