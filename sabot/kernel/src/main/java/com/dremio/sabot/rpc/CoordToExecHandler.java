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
package com.dremio.sabot.rpc;

import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.rpc.RpcException;

/**
 * Handler for messages going from coordinator to executor.
 */
public interface CoordToExecHandler {

  void startFragments(InitializeFragments fragments) throws RpcException;

  void cancelFragment(FragmentHandle fragments) throws RpcException;
}