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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.DeleteHandler;
import com.dremio.exec.planner.sql.handlers.query.InsertTableHandler;
import com.dremio.exec.planner.sql.handlers.query.MergeHandler;
import com.dremio.exec.planner.sql.handlers.query.NormalHandler;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.UpdateHandler;


public class ExplainHandler implements SqlDirectHandler<ExplainHandler.Explain> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainHandler.class);

  private final SqlHandlerConfig config;

  public ExplainHandler(SqlHandlerConfig config) {
    super();
    this.config = new SqlHandlerConfig(config.getContext(), config.getConverter(), config.getObserver(),
      config.getMaterializations().orElse(null));
  }

  @Override
  public List<Explain> toResult(String sql, SqlNode sqlNode) throws Exception {
    try {
      final SqlExplain node = SqlNodeUtil.unwrap(sqlNode, SqlExplain.class);
      final SqlLiteral op = node.operand(2);
      final SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();

      final SqlExplainLevel level = node.getDetailLevel() != null
        ? node.getDetailLevel()
        : SqlExplainLevel.ALL_ATTRIBUTES;
      final ResultMode mode;
      switch (depth) {
        case LOGICAL:
          mode = ResultMode.LOGICAL;
          break;
        case PHYSICAL:
          mode = ResultMode.PHYSICAL;
          break;
        default:
          throw new UnsupportedOperationException("Unknown depth " + depth);
      }

      // get plan
      final SqlNode innerNode = node.operand(0);
      SqlToPlanHandler innerNodeHandler;
      switch (innerNode.getKind()) {
        // We currently only support OrderedQueryOrExpr and Insert/Delete/Update/Merge
        case INSERT:
          innerNodeHandler = new InsertTableHandler();
          break;
        case DELETE:
          innerNodeHandler = new DeleteHandler();
          break;
        case MERGE:
          innerNodeHandler = new MergeHandler();
          break;
        case UPDATE:
          innerNodeHandler = new UpdateHandler();
          break;
        // for OrderedQueryOrExpr such as select, use NormalHandler
        default:
          innerNodeHandler = new NormalHandler();
      }

      innerNodeHandler
        .getPlan(
          config,
          innerNode.toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
          innerNode);

      String planAsText;
      if (mode == ResultMode.LOGICAL) {
        planAsText = RelOptUtil.toString(innerNodeHandler.getLogicalPlan(), level);
      } else {
        planAsText = innerNodeHandler.getTextPlan();
      }

      Explain explain = new Explain(planAsText);
      return Collections.singletonList(explain);
    } catch (Exception ex) {
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  public static class Explain {
    public final String text;

    public Explain(String text) {
      super();
      this.text = text;
    }
  }

  @Override
  public Class<Explain> getResultType() {
    return Explain.class;
  }
}
