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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlAlterClearPlanCache;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.rpc.user.UserSession;

/**
 * Handles "ALTER .. CLEAR PLAN CACHE statements
 */
public class AlterClearPlanCacheHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlterClearPlanCacheHandler.class);

  private final QueryContext context;
  private final UserSession session;

  public AlterClearPlanCacheHandler(QueryContext context) {
    super();
    this.context = context;
    this.session = context.getSession();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws ValidationException, RelConversionException, IOException,
      ForemanSetupException {
    final SqlAlterClearPlanCache option = SqlNodeUtil.unwrap(sqlNode, SqlAlterClearPlanCache.class);

    final String scope = option.getScope();
    final OptionType type;
    if (scope == null) { // No scope mentioned assumed SESSION; not supported by CLEAN PLAN CACHE for now
      throw UserException.validationError()
        .message("Scope not provided and assumed SESSION. Scope must be SYSTEM for CLEAR PLAN CACHE.")
        .build(logger);
    } else {
      switch (scope.toLowerCase()) {
        case "system":
          type = OptionType.SYSTEM;
          break;
        default:
          throw UserException.validationError()
              .message("Invalid OPTION scope %s. Scope must be SYSTEM.", scope)
              .build(logger);
      }
    }
    context.getPlanCache().getCachePlans().invalidateAll();
    return Collections.singletonList(SimpleCommandResult.successful("Plan cache cleared."));
  }

}
