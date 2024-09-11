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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.record.BatchSchema;

/** Take a sql node and return as a prepared statement response. */
public class HandlerToPrepareDirect
    extends HandlerToPrepareDirectBase<CreatePreparedStatementResp> {

  public HandlerToPrepareDirect(String sql, QueryContext context, SqlDirectHandler<?> handler) {
    super(sql, context, handler);
  }

  @Override
  public CreatePreparedStatementResp execute() throws Exception {
    final ServerPreparedStatementState state =
        ServerPreparedStatementState.newBuilder().setHandle(-1).setSqlQuery(getSql()).build();
    final QueryContext context = getContext();
    return PreparedStatementProvider.build(
        getSchema(),
        state,
        context.getQueryId(),
        context.getSession().getCatalogName(),
        context.getSession().getRecordBatchFormat(),
        BatchSchema.EMPTY);
  }
}
