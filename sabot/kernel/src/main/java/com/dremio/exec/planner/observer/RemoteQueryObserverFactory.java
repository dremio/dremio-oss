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

import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.work.protector.UserResponseHandler;
import com.dremio.exec.work.protector.UserResult;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.rpc.user.UserSession;

public class RemoteQueryObserverFactory implements QueryObserverFactory {

  @Override
  public QueryObserver createNewQueryObserver(ExternalId id, UserSession session, UserResponseHandler handler) {
    return new RemoteQueryObserver(id, handler);
  }

  public static class RemoteQueryObserver extends AbstractQueryObserver {

    private final ExternalId externalId;
    private final UserResponseHandler handler;

    public RemoteQueryObserver(final ExternalId externalId, final UserResponseHandler handler) {
      this.externalId = externalId;
      this.handler = handler;
    }

    @Override
    public AttemptObserver newAttempt(AttemptId attemptId, AttemptReason reason) {
      return new RemoteAttemptObserver(externalId, handler);
    }

    @Override
    public void execCompletion(UserResult result) {
      handler.completed(result.withNewQueryId(ExternalIdHelper.toQueryId(externalId)));
    }
  }
}
