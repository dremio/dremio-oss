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
package com.dremio.exec.planner.sql.handlers.direct;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.planner.sql.parser.SqlUseBranch;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Handler for setting current default branch.
 */
public final class UseBranchHandler extends UseVersionHandler {
  public UseBranchHandler(UserSession userSession, OptionResolver optionResolver) {
    super(userSession, optionResolver);
  }

  @Override
  public VersionContext getVersionContextFromSqlNode(SqlNode sqlNode) throws ForemanSetupException {
    final String branchName = SqlNodeUtil.unwrap(sqlNode, SqlUseBranch.class).getBranchName().toString();
    return VersionContext.fromBranchName(branchName);
  }
}
