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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.pojo.PojoRecordReader;

/**
 * Take a sql node and return as a prepared statement response.
 */
public class HandlerToPrepareDirect implements CommandRunner<CreatePreparedStatementResp> {

  private final SqlDirectHandler<?> handler;
  private final String sql;
  private final QueryContext context;

  private BatchSchema schema;

  public HandlerToPrepareDirect(String sql, QueryContext context, SqlDirectHandler<?> handler) {
    super();
    this.handler = handler;
    this.sql = sql;
    this.context = context;
  }

  @Override
  public double plan() throws Exception {
    schema = PojoRecordReader.getSchema(handler.getResultType());
    // we won't save in the prepared statement cache. It isn't worth it for a direct plan.
    return 1;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  @Override
  public CreatePreparedStatementResp execute() throws Exception {
    ServerPreparedStatementState state = ServerPreparedStatementState.newBuilder().setHandle(-1).setSqlQuery(sql).build();
    return PreparedStatementProvider.build(schema, state, context.getQueryId(), context.getSession().getCatalogName());
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SYNC_RESPONSE;
  }

  @Override
  public String getDescription() {
    return "prepare; direct";
  }

}
