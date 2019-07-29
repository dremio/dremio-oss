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
package com.dremio.exec.planner.observer;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.work.protector.UserResponseHandler;

public class RemoteAttemptObserver extends AbstractAttemptObserver {

  private final UserResponseHandler handler;
  private final ExternalId externalId;


  public RemoteAttemptObserver(final ExternalId externalId, UserResponseHandler handler) {
    this.handler = handler;
    this.externalId = externalId;
  }

  @Override
  public void execDataArrived(RpcOutcomeListener<Ack> outcomeListener, QueryWritableBatch batch) {
    handler.sendData(outcomeListener, ExternalIdHelper.replaceQueryId(batch, externalId));
  }

  @Override
  public void planParallelized(PlanningSet planningSet) {
    handler.planParallelized(planningSet);
  }

}
