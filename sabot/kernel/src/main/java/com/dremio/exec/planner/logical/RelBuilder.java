/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

public class RelBuilder extends org.apache.calcite.tools.RelBuilder {

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return new RelBuilderFactory() {
      @Override
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        return new RelBuilder(context, cluster, schema);
      }
    };
  }

  public static org.apache.calcite.tools.RelBuilder newCalciteRelBuilderWithoutContext(RelOptCluster cluster) {
    return proto((RelNode.Context) null).create(cluster, null);
  }

  protected RelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  /** Creates a relational expression that reads from an input and throws
   *  all of the rows away.
   */
  @Override
  public RelBuilder empty() {
    final Frame frame = stack.pop();
    final RelNode input;
    // If the rel that we are limiting the output of a rel, we should just add a limit 0 on top.
    // If the rel that we are limiting is a Filter replace it as well since Filter does not
    // change the row type.
    if (!(frame.rel instanceof Filter)) {
      input = frame.rel;
    } else {
      input = frame.rel.getInput(0);
    }
    final RelNode sort = sortFactory.createSort(input, RelCollations.EMPTY,
      frame.rel.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
      frame.rel.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
    push(sort);
    return this;
  }



  //TODO: Incorporate CALCITE-1610.
  /** Creates a {@link Sort} by a list of expressions, with limit and offset.
  *
  * @param offset Number of rows to skip; non-positive means don't skip any
  * @param fetch Maximum number of rows to fetch; negative means no limit
  * @param nodes Sort expressions
  */
 public RelBuilder sortLimit(int offset, int fetch,
     Iterable<? extends RexNode> nodes) {
   final List<RelFieldCollation> fieldCollations = new ArrayList<>();
   final RelDataType inputRowType = peek().getRowType();
   final List<RexNode> extraNodes = projects(inputRowType);
   final List<RexNode> originalExtraNodes = ImmutableList.copyOf(extraNodes);
   for (RexNode node : nodes) {
     fieldCollations.add(
         collation(node, RelFieldCollation.Direction.ASCENDING, null,
             extraNodes));
   }
   final RexNode offsetNode = offset <= 0 ? null : literal(offset);
   final RexNode fetchNode = fetch < 0 ? null : literal(fetch);
   if (offsetNode == null && fetch == 0) {
     return empty();
   }
   if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
     return this; // sort is trivial
   }

   final boolean addedFields = extraNodes.size() > originalExtraNodes.size();
   if (fieldCollations.isEmpty()) {
     assert !addedFields;
     RelNode top = peek();
     if (top instanceof Sort) {
       final Sort sort2 = (Sort) top;
       if (sort2.offset == null && sort2.fetch == null) {
         replaceTop(sort2.getInput());
         final RelNode sort =
             sortFactory.createSort(peek(), sort2.collation,
                 offsetNode, fetchNode);
         replaceTop(sort);
         return this;
       }
     }
     if (top instanceof Project) {
       final Project project = (Project) top;
       if (project.getInput() instanceof Sort) {
         final Sort sort2 = (Sort) project.getInput();
         if (sort2.offset == null && sort2.fetch == null) {
           final RelNode sort =
               sortFactory.createSort(sort2.getInput(), sort2.collation,
                   offsetNode, fetchNode);
           replaceTop(
               projectFactory.createProject(sort,
                   project.getProjects(),
                   Pair.right(project.getNamedProjects())));
           return this;
         }
       }
     }
   }
   if (addedFields) {
     project(extraNodes);
   }
   final RelNode sort =
       sortFactory.createSort(peek(), RelCollations.of(fieldCollations),
           offsetNode, fetchNode);
   replaceTop(sort);
   if (addedFields) {
     project(originalExtraNodes);
   }
   return this;
 }

  private List<RexNode> projects(RelDataType inputRowType) {
    final List<RexNode> exprList = new ArrayList<>();
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      exprList.add(rexBuilder.makeInputRef(field.getType(), field.getIndex()));
    }
    return exprList;
  }

  private void replaceTop(RelNode node) {
    stack.pop();
    push(node);
  }

  private static RelFieldCollation collation(RexNode node,
      RelFieldCollation.Direction direction,
      RelFieldCollation.NullDirection nullDirection, List<RexNode> extraNodes) {
    switch (node.getKind()) {
    case INPUT_REF:
      return new RelFieldCollation(((RexInputRef) node).getIndex(), direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    case DESCENDING:
      return collation(((RexCall) node).getOperands().get(0),
          RelFieldCollation.Direction.DESCENDING,
          nullDirection, extraNodes);
    case NULLS_FIRST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.FIRST, extraNodes);
    case NULLS_LAST:
      return collation(((RexCall) node).getOperands().get(0), direction,
          RelFieldCollation.NullDirection.LAST, extraNodes);
    default:
      final int fieldIndex = extraNodes.size();
      extraNodes.add(node);
      return new RelFieldCollation(fieldIndex, direction,
          Util.first(nullDirection, direction.defaultNullDirection()));
    }
  }
}
