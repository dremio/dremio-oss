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
package com.dremio.exec.planner.sql.handlers.direct;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.planner.sql.parser.SqlUseSchema;
import com.dremio.sabot.rpc.user.UserSession;

public class UseSchemaHandler extends SimpleDirectHandler {

  private final UserSession session;
  private final SchemaPlus defaultSchema;

  public UseSchemaHandler(UserSession session, SchemaPlus defaultSchema) {
    this.session = session;
    this.defaultSchema = defaultSchema;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlUseSchema useSchema = SqlNodeUtil.unwrap(sqlNode, SqlUseSchema.class);
    final String newDefaultSchemaPath = useSchema.getSchema();

    session.setDefaultSchemaPath(newDefaultSchemaPath, defaultSchema);
    return Collections.singletonList(SimpleCommandResult.successful("Default schema changed to [%s]", session.getDefaultSchemaPath()));
  }
}
