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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;

public class ExplainHandler implements SqlDirectHandler<ExplainHandler.Explain> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainHandler.class);

  private final SqlHandlerConfig config;

  public ExplainHandler(SqlHandlerConfig config) {
    super();
    this.config = new SqlHandlerConfig(config.getContext(), config.getConverter(), config.getObserver(),
      config.getMaterializations().orNull());
  }

  @Override
  public List<Explain> toResult(String sql, SqlNode sqlNode) throws Exception {
    try {
      final SqlExplain node = SqlNodeUtil.unwrap(sqlNode, SqlExplain.class);
      final SqlLiteral op = node.operand(2);
      final SqlExplain.Depth depth = (SqlExplain.Depth) op.getValue();

      final ResultMode mode;
      SqlExplainLevel level = SqlExplainLevel.ALL_ATTRIBUTES;

      if (node.getDetailLevel() != null) {
        level = node.getDetailLevel();
      }

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

      final SqlNode innerNode = node.operand(0);

//      try(DisabledBlock block = toggle.openDisabledBlock()){
        Rel drel;
        final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, innerNode);
        final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
        final RelNode queryRelNode = convertedRelNode.getConvertedNode();

        PrelTransformer.log("Calcite", queryRelNode, logger, null);
        drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);

        if (mode == ResultMode.LOGICAL) {
          return Collections.singletonList(new Explain(RelOptUtil.toString(drel, level)));
        }

        final Pair<Prel, String> convertToPrel = PrelTransformer.convertToPrel(config, drel);
        final String text = convertToPrel.getValue();
        return Collections.singletonList(new Explain(text));
//      }
    } catch (Exception ex){
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
