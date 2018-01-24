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
package com.dremio.exec.work.protector;

import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.sabot.rpc.user.UserSession;

public interface UserWorker {

  ExternalId submitWork(UserSession session, UserResponseHandler responseHandler, UserRequest request, TerminationListenerRegistry registry);

  Ack cancelQuery(ExternalId query);

  Ack resumeQuery(ExternalId query);

  OptionManager getSystemOptions();

}