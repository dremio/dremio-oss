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
package com.dremio.sabot.exec.rpc;

import com.dremio.exec.proto.CoordExecRPC.InitializeFragments;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.sabot.exec.EventProvider;
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
  public void startFragments(InitializeFragments fragments) throws UserRpcException {
    for(int i = 0; i < fragments.getFragmentCount(); i++) {
      startFragment(fragments.getFragment(i));
    }
  }

  private void startFragment(final PlanFragment fragment) throws UserRpcException {
    logger.info("Received remote fragment start instruction for {}", QueryIdHelper.getQueryIdentifier(fragment.getHandle()));

    try {
      final EventProvider eventProvider = fragmentExecutors.getEventProvider(fragment.getHandle());
      fragmentExecutors.startFragment(builder.build(fragment, eventProvider));

    } catch (final Exception e) {
        throw new UserRpcException(identity, "Failure while trying to start remote fragment", e);
    } catch (final OutOfMemoryError t) {
      if (t.getMessage().startsWith("Direct buffer")) {
        throw new UserRpcException(identity, "Out of direct memory while trying to start remote fragment", t);
      } else {
        throw t;
      }
    }
  }

  /* (non-Javadoc)
   * @see com.dremio.exec.work.batch.BitComHandler#cancelFragment(com.dremio.exec.proto.ExecProtos.FragmentHandle)
   */
  public void cancelFragment(final FragmentHandle handle) {
    fragmentExecutors.cancel(handle);
  }
}
