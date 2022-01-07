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

import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.rpc.user.UserSession;

public abstract class UseVersionHandler extends SimpleDirectHandler {
  private final UserSession userSession;
  private final OptionResolver optionResolver;

  public UseVersionHandler(UserSession userSession, OptionResolver optionResolver) {
    this.userSession = Preconditions.checkNotNull(userSession);
    this.optionResolver = Preconditions.checkNotNull(optionResolver);
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws ForemanSetupException {
    if (!optionResolver.getOption(ENABLE_USE_VERSION_SYNTAX)) {
      throw UserException.unsupportedError()
        .message("USE VERSION (BRANCH|TAG|COMMIT) syntax is not supported.")
        .buildSilently();
    }

    userSession.setVersionContext(Optional.of(getVersionContextFromSqlNode(sqlNode)));

    String version = userSession.getVersionContext().isPresent() ? userSession.getVersionContext().get().toString() : "NOT SET";
    return Collections.singletonList(SimpleCommandResult.successful("Default version set to: %s", version));
  }

  public abstract VersionContext getVersionContextFromSqlNode(SqlNode sqlNode) throws ForemanSetupException;
}
