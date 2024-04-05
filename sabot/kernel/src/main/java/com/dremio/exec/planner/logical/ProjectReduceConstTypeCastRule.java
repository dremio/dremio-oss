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

package com.dremio.exec.planner.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 *
 *
 * <pre>
 * For a given query:
 * SELECT
 *    TS3,
 *    TIMESTAMPADD(hour,1,TS3),
 *    DATE_ADD(TS3,1),
 *    EXTRACT(HOUR FROM TS3)
 * FROM T3
 * WHERE TS3 = '2023-02-03 12:30:00';
 *
 * where TS3 is a column of type TIMESTAMP in table T3. Dremio allows the const string '2023-02-01 12:30:00'
 * be used to filter for column TS3 without explicitly casting it to TIMESTAMP. We have a query optimization
 * rule that pulls up the const string '2023-02-01 12:30:00' to the SELECT expressions list.
 * It can be understood like below:
 * SELECT
 *    TS3,
 *    TIMESTAMPADD(hour,1,'2023-02-03 12:30:00'),
 *    DATE_ADD('2023-02-03 12:30:00',1),
 *    EXTRACT(HOUR FROM '2023-02-03 12:30:00')
 * FROM T3
 * WHERE TS3 = '2023-02-03 12:30:00';
 *
 * However, the functions like TIMESTAMPADD, DATE_ADD and EXTRACT are not accepting strings as timestamp parameters.
 * The default behavior of Dremio function lookup is to find the best match for the string parameters. It turns out
 * that the string parameters are implicitly cast to DATE type. So, the output of the query is wrong due to the
 * truncated time portion of  '2023-02-03 12:30:00'.
 * We are trying to add type cast for the const, The final effect can be understood as below
 * SELECT
 *    TS3,
 *    TIMESTAMPADD(hour,1,TIMESTAMP '2023-02-03 12:30:00'),
 *    DATE_ADD(TIMESTAMP '2023-02-03 12:30:00',1),
 *    EXTRACT(HOUR FROM TIMESTAMP '2023-02-03 12:30:00')
 * FROM T3
 * WHERE TS3 = '2023-02-03 12:30:00';
 * </pre>
 */
public final class ProjectReduceConstTypeCastRule extends RelOptRule {
  public static final ProjectReduceConstTypeCastRule INSTANCE =
      new ProjectReduceConstTypeCastRule();

  private ProjectReduceConstTypeCastRule() {
    super(operand(LogicalProject.class, any()), "ProjectReduceConstTypeCastRule");
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    Project project = relOptRuleCall.rel(0);
    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelMetadataQuery mq = relOptRuleCall.getMetadataQuery();
    RelOptPredicateList predicates = mq.getPulledUpPredicates(project.getInput());
    if (predicates != null && !predicates.constantMap.isEmpty()) {
      List<Map.Entry<RexNode, RexNode>> pairs = new ArrayList<>(predicates.constantMap.entrySet());
      RexReplacer replacer = new RexReplacer(rexBuilder, Pair.left(pairs), Pair.right(pairs));
      RelNode rewrittenQuery = project.accept(replacer);
      if (rewrittenQuery != project) {
        relOptRuleCall.transformTo(rewrittenQuery);
      }
    }
  }

  protected static class RexReplacer extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final List<RexNode> replaceableExps;
    private final List<RexNode> replacementExps;

    RexReplacer(
        RexBuilder rexBuilder, List<RexNode> replaceableExps, List<RexNode> replacementExps) {
      this.rexBuilder = rexBuilder;
      this.replaceableExps = replaceableExps;
      this.replacementExps = replacementExps;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      RexNode node = addCast(inputRef);
      return node == null ? super.visitInputRef(inputRef) : node;
    }

    private RexNode addCast(RexInputRef inputRef) {
      int i = replaceableExps.indexOf(inputRef);
      if (i == -1) {
        return null;
      }

      RexNode replacement = replacementExps.get(i);

      // Currently we only reduce/add cast for TIMESTAMP
      if (inputRef.getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP)
          && replacement.getType() != inputRef.getType()) {
        return rexBuilder.makeAbstractCast(inputRef.getType(), replacement);
      }

      return null;
    }
  }
}
