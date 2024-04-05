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

import com.dremio.exec.planner.common.JoinRelBase;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;

/** Logical Join implemented in Dremio. */
public class JoinRel extends JoinRelBase implements Rel {
  /**
   * Creates a JoinRel. We do not throw InvalidRelException in Logical planning phase. It's up to
   * the post-logical planning check or physical planning to detect the unsupported join type, and
   * throw exception.
   */
  private JoinRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    this(cluster, traits, left, right, condition, joinType, false);
  }

  private JoinRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      boolean allowRowTypeMismatch) {
    super(cluster, traits, left, right, condition, joinType, allowRowTypeMismatch);
    assert traits.contains(Rel.LOGICAL);
  }

  public static JoinRel create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    final RelTraitSet traits = adjustTraits(traitSet);

    return new JoinRel(cluster, traits, left, right, condition, joinType);
  }

  public static JoinRel create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType,
      boolean allowRowTypeMismatch) {
    final RelTraitSet traits = adjustTraits(traitSet);

    return new JoinRel(cluster, traits, left, right, condition, joinType, allowRowTypeMismatch);
  }

  @Override
  public boolean isValid(Litmus litmus, Context context) {
    if (condition != null) {
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        return litmus.fail("condition must be boolean: {}", condition.getType());
      }

      RexChecker checker =
          new RexChecker(
              getCluster()
                  .getTypeFactory()
                  .builder()
                  .addAll(getSystemFieldList())
                  .addAll(getLeft().getRowType().getFieldList())
                  .addAll(getRight().getRowType().getFieldList())
                  .build(),
              context,
              litmus);
      condition.accept(checker);
      if (checker.getFailureCount() > 0) {
        return litmus.fail(checker.getFailureCount() + " failures in condition " + condition);
      }
    }
    return litmus.succeed();
  }

  @Override
  public JoinRel copy(
      RelTraitSet traitSet,
      RexNode condition,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new JoinRel(getCluster(), traitSet, left, right, condition, joinType);
  }
}
