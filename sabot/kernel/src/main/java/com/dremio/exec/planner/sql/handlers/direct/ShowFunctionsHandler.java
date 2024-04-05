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

import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlShowFunctions;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.work.foreman.ForemanSetupException;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.calcite.sql.SqlNode;

/**
 * Handler for show Functions.
 *
 * <p>SHOW Functions [ LIKE 'pattern' ]
 */
public class ShowFunctionsHandler
    implements SqlDirectHandler<ShowFunctionsHandler.ShowFunctionResult> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ShowFunctionsHandler.class);

  private final UserDefinedFunctionCatalog userDefinedFunctionCatalog;

  public ShowFunctionsHandler(QueryContext context) {
    userDefinedFunctionCatalog = context.getUserDefinedFunctionCatalog();
  }

  @Override
  public List<ShowFunctionResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    return showFunctions(sqlNode);
  }

  private List<ShowFunctionResult> showFunctions(SqlNode sqlNode) throws ForemanSetupException {
    final SqlShowFunctions sqlShowFunctions = SqlNodeUtil.unwrap(sqlNode, SqlShowFunctions.class);

    Stream<UserDefinedFunction> functions =
        StreamSupport.stream(userDefinedFunctionCatalog.getAllFunctions().spliterator(), false);
    if (sqlShowFunctions.getLikePattern() != null) {
      final Pattern likePattern = SqlNodeUtil.getPattern(sqlShowFunctions.getLikePattern());
      final Matcher m = likePattern.matcher("");
      functions = functions.filter(function -> m.reset(function.getName()).matches());
    }

    functions = functions.sorted(Comparator.comparing(UserDefinedFunction::getName));

    return functions
        .map(function -> new ShowFunctionResult(function.getName()))
        .collect(Collectors.toList());
  }

  @Override
  public Class<ShowFunctionResult> getResultType() {
    return ShowFunctionResult.class;
  }

  public static class ShowFunctionResult {
    public final String FUNCTION_NAME;

    public ShowFunctionResult(String functionName) {
      super();
      FUNCTION_NAME = functionName;
    }

    @Override
    public String toString() {
      return "ShowFunctionResult{" + ", Function_NAME='" + FUNCTION_NAME + '\'' + '}';
    }
  }
}
