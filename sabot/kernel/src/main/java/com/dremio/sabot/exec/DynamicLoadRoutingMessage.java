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

package com.dremio.sabot.exec;

import com.dremio.exec.proto.ExecRPC.DlrProtoMessage;
import com.dremio.exec.proto.UserBitShared.QueryId;

public class DynamicLoadRoutingMessage {
  private final QueryId queryId;
  private final int command;

  DynamicLoadRoutingMessage(QueryId queryId, int command) {
    this.queryId = queryId;
    this.command = command;
  }

  public DynamicLoadRoutingMessage(final DlrProtoMessage message) {
    this.queryId = message.getQueryId();
    this.command = message.getCommand();
  }

  public DlrProtoMessage toProtoMessage() {
    DlrProtoMessage.Builder builder = DlrProtoMessage.newBuilder();

    builder.setQueryId(queryId);
    builder.setCommand(command);

    return builder.build();
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getCommand() {
    return command;
  }
}
