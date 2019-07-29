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
package com.dremio.sabot.exec.rpc;

import com.dremio.exec.proto.CoordExecRPC.ActivateFragments;
import com.dremio.exec.proto.CoordExecRPC.CancelFragments;
import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.sabot.exec.FragmentExecutors;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.rpc.CoordToExecHandler;

public class CoordToExecHandlerImpl implements CoordToExecHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CoordToExecHandlerImpl.class);

  private final NodeEndpoint identity;
  private final FragmentExecutors fragmentExecutors;
  private final FragmentExecutorBuilder builder;

  public CoordToExecHandlerImpl(
      NodeEndpoint identity,
      FragmentExecutors fragmentExecutors,
      FragmentExecutorBuilder builder) {
    super();
    this.identity = identity;
    this.fragmentExecutors = fragmentExecutors;
    this.builder = builder;
  }

  @Override
  public void startFragments(InitializeFragments fragments, ResponseSender sender) throws UserRpcException {
    fragmentExecutors.startFragments(fragments, builder, sender, identity);
  }

  @Override
  public void activateFragments(ActivateFragments fragments) {
    fragmentExecutors.activateFragments(fragments.getQueryId(), builder.getClerk());
  }

  @Override
  public void cancelFragments(CancelFragments fragments) {
    fragmentExecutors.cancelFragments(fragments.getQueryId(), builder.getClerk());
  }
}
