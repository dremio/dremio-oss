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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.serialization.LogicalPlanSerializer;
import com.dremio.exec.planner.serialization.RelSerializerFactory;
import com.dremio.exec.planner.sql.SqlExceptionHelper;
import com.dremio.exec.planner.sql.handlers.ConvertedRelNode;
import com.dremio.exec.planner.sql.handlers.PrelTransformer;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.ViewAccessEvaluator;
import com.dremio.exec.planner.sql.parser.SqlExplainJson;

/**
 * Handler for EXPLAIN JSON commands.
 */
public class ExplainJsonHandler implements SqlDirectHandler<ExplainJsonHandler.Explain> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExplainJsonHandler.class);

  private final SqlHandlerConfig config;

  public ExplainJsonHandler(SqlHandlerConfig config) {
    super();
    this.config = new SqlHandlerConfig(config.getContext(), config.getConverter(), config.getObserver(),
      config.getMaterializations().orNull());
  }

  @Override
  public List<Explain> toResult(String sql, SqlNode sqlNode) throws Exception {
    Observer observer = new Observer();
    config.addObserver(observer);

    try {
      final SqlExplainJson node = SqlNodeUtil.unwrap(sqlNode, SqlExplainJson.class);
      final SqlNode innerNode = node.getQuery();

      Rel drel;
      final ConvertedRelNode convertedRelNode = PrelTransformer.validateAndConvert(config, innerNode);
      final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
      final RelNode queryRelNode = convertedRelNode.getConvertedNode();
      ViewAccessEvaluator viewAccessEvaluator = null;
      if (config.getConverter().getSubstitutionProvider().isDefaultRawReflectionEnabled()) {
        final RelNode convertedRelWithExpansionNodes = ((DremioVolcanoPlanner) queryRelNode.getCluster().getPlanner()).getOriginalRoot();
        viewAccessEvaluator = new ViewAccessEvaluator(convertedRelWithExpansionNodes, config);
        config.getContext().getExecutorService().submit(viewAccessEvaluator);
      }
      drel = PrelTransformer.convertToDrel(config, queryRelNode, validatedRowType);
      PrelTransformer.convertToPrel(config, drel);

      if (viewAccessEvaluator != null) {
        viewAccessEvaluator.getLatch().await(config.getContext().getPlannerSettings().getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
        if (viewAccessEvaluator.getException() != null) {
          throw viewAccessEvaluator.getException();
        }
      }

      return toResultInner(node.getPhase(), observer.nodes);
    } catch (Exception ex){
      throw SqlExceptionHelper.coerceException(logger, sql, ex, true);
    }
  }

  private List<Explain> toResultInner(String phase, List<TransformedNode> nodes) {
    final RelSerializerFactory factory = RelSerializerFactory.getPlanningFactory(config.getContext().getConfig(), config.getContext().getScanResult());
    final LogicalPlanSerializer serializer = factory.getSerializer(config.getConverter().getCluster());

    for (TransformedNode n : nodes) {
      if(n.getPhase().equalsIgnoreCase(phase)) {
        return Collections.singletonList(new Explain(serializer.serializeToJson(n.getNode())));
      }
    }

    throw UserException.validationError()
      .message("Unknown phase: %s", phase)
      .build(logger);
  }

  private static class TransformedNode {
    private final String phase;
    private final RelNode node;

    public TransformedNode(String phase, RelNode node) {
      super();
      this.phase = phase;
      this.node = node;
    }

    public String getPhase() {
      return phase;
    }

    public RelNode getNode() {
      return node;
    }

  }

  private static class Observer extends AbstractAttemptObserver {

    private List<TransformedNode> nodes = new ArrayList<>();

    @Override
    public void planConvertedToRel(RelNode converted, long millisTaken) {
      add("ORIGINAL", converted);
    }

    @Override
    public void finalPrel(Prel prel) {
      add("PHYSICAL", prel);
    }

    private void add(String phase, RelNode node) {
      nodes.add(new TransformedNode(phase, node));
    }

    @Override
    public void planRelTransform(PlannerPhase phase, RelOptPlanner planner, RelNode before, RelNode after,
        long millisTaken) {
      add(phase.name(), after);
    }

  }

  public static class Explain {

    public final String json;

    public Explain(String json) {
      super();
      this.json = json;
    }

  }

  @Override
  public Class<Explain> getResultType() {
    return Explain.class;
  }
}
