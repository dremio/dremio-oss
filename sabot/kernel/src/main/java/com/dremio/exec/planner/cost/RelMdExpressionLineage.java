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
package com.dremio.exec.planner.cost;

import com.dremio.exec.planner.physical.SelectionVectorRemoverPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

/** Override {@link RelMdExpressionLineage} */
public class RelMdExpressionLineage extends org.apache.calcite.rel.metadata.RelMdExpressionLineage {

  private static final RelMdExpressionLineage INSTANCE = new RelMdExpressionLineage();

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.EXPRESSION_LINEAGE.method, INSTANCE);

  public Set<RexNode> getExpressionLineage(
      SelectionVectorRemoverPrel rel, RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  public Set<RexNode> getExpressionLineage(
      TableFunctionPrel rel, RelMetadataQuery mq, RexNode outputExpression) {

    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexNode inputRef =
          RexTableInputRef.of(
              RexTableInputRef.RelTableRef.of(rel.getTable(), 0),
              RexInputRef.of(idx, rel.getRowType().getFieldList()));
      final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
      mapping.put(ref, ImmutableSet.of(inputRef));
    }
    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  private static ImmutableBitSet extractInputRefs(RexNode expr) {
    Set<RelDataTypeField> inputExtraFields = new LinkedHashSet();
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    expr.accept(inputFinder);
    return inputFinder.build();
  }
}
