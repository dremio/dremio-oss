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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AGG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;

/**
 * The general form of a Correlated Uncollect query is:
 *
 * Correlate
 *    TableScan(myTable)
 *    Series Of Transformation Operators
 *      Flatten
 *        Project(array_to_uncollect=[$corX.column_name])
 *          Values(tuples=[[{ 0 }]])
 *
 * We want to rewrite this to:
 *
 * Join on $index_matching_the_correlated_column = myTable.numColumns() + 1
 *    TableScan(myTable)
 *    Series Of Transformation Operators with modified refinputs
 *      Flatten($1)
 *        Project($index_matching_the_correlated_column, $index_matching_the_correlated_column)
 *          TableScan(myTable)
 *
 * Basically we are creating a secondary table with the array and it's array items unnested,
 * then join the first table on it using the array as the join key.
 *
 * For example suppose the query is:
 *
 * SELECT * FROM "arrays.json", UNNEST(int_array)
 *
 * And arrays.json has a single column called int_array:
 * [[1, 2, 3]]
 * [[4, 5, 6]]
 *
 * Then we are creating the following table using flatten:
 * [[1, 2, 3], 1]
 * [[1, 2, 3], 2]
 * [[1, 2, 3], 3]
 * [[4, 5, 6], 4]
 * [[4, 5, 6], 5]
 * [[4, 5, 6], 6]
 *
 * And we join where the int_array columns are equal in both tables.
 * Note that we also GROUP BY the array_column if there is an aggregation like ARRAY_AGG
 * and join against that.
 */
public final class FlattenDecorrelator extends RelHomogeneousShuttle {
  private final RelBuilder relBuilder;

  private FlattenDecorrelator(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
  }

  public static RelNode decorrelate(RelNode relNode, RelBuilder relBuilder) {
    return relNode.accept(new FlattenDecorrelator(relBuilder));
  }

  @Override
  public RelNode visit(RelNode relNode) {
    if (relNode instanceof Correlate) {
      return visitCorrelate((Correlate) relNode);
    }

    return super.visit(relNode);
  }

  public RelNode visitCorrelate(Correlate correlate) {
    // We are looking for the following pattern:
    /*
     * Correlate
     *    TableScan(myTable)
     *    Series Of Transformation Operators
     *      Flatten
     *        Project(array_to_uncollect=[$corX.column_name])
     *          Values(tuples=[[{ 0 }]])
     */

    if (hasCorrelate(correlate.getLeft()) || hasCorrelate(correlate.getRight())) {
      // Recurse for nested correlates
      correlate = (Correlate) super.visit(correlate);
    }

    RelNode left = correlate.getLeft();
    RelNode right = correlate.getRight();

    if (!hasFlatten(right)) {
      // We only care about correlates with flatten;
      return correlate;
    }

    right = right.accept(new AggCallNameSetter(relBuilder));

    Map<RelNode, Set<RexFieldAccess>> correlateVariableMapping = RexSubQueryUtils.mapCorrelateVariables(right);
    RelNode rewrittenRight = right.accept(new FlattenInputReplacer(left, relBuilder, correlateVariableMapping));

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    List<RexNode> equiJoins = correlate.getRequiredColumns().asList()
      .stream()
      .map(requiredColumn -> {
        // Find the name of the required column
        String name = left.getRowType().getFieldList().get(requiredColumn).getName();
        // Find the index of it in the right table
        int rightIndex = rewrittenRight.getRowType().getFieldList()
          .stream()
          .filter(field -> field.getName().equals(name))
          .findFirst()
          .get()
          .getIndex();

        RexNode equiJoin = rexBuilder.makeCall(
          SqlStdOperatorTable.EQUALS,
          rexBuilder.makeInputRef(left, requiredColumn),
          rexBuilder.makeInputRef(
            rewrittenRight.getRowType().getFieldList().get(rightIndex).getType(),
            left.getRowType().getFieldCount() + rightIndex));
        return equiJoin;
      })
      .collect(Collectors.toList());
    RexNode joinCondition = RexUtil.composeConjunction(rexBuilder, equiJoins);

    RelNode rewrittenNode = relBuilder
      .push(left)
      .push(rewrittenRight)
      .join(correlate.getJoinType(), joinCondition)
      .build();

    // We need to remove the extra array introduced as a join key + correlate variables for the flatten operator
    List<RexNode> trimmed = new ArrayList<>();
    List<String> aliases = new ArrayList<>();
    for (int i = 0; i < left.getRowType().getFieldCount(); i++) {
      trimmed.add(rexBuilder.makeInputRef(rewrittenNode, i));
      aliases.add(rewrittenNode.getRowType().getFieldNames().get(i));
    }

    for (int i = correlate.getRequiredColumns().cardinality(); i < rewrittenRight.getRowType().getFieldCount(); i++) {
      trimmed.add(rexBuilder.makeInputRef(rewrittenNode, left.getRowType().getFieldCount() + i));
      aliases.add(rewrittenNode.getRowType().getFieldNames().get(left.getRowType().getFieldCount() + i));
    }

    rewrittenNode = relBuilder
      .push(rewrittenNode)
      .project(trimmed, aliases)
      .build();

    return rewrittenNode;
  }

  /**
   * Some AggCalls have null for the name field.
   * This breaks the decorrelation logic when we do ref index realignment.
   * This class goes through and sets the name field to a unique name.
   */
  private static final class AggCallNameSetter extends StatelessRelShuttleImpl {
    private final RelBuilder relBuilder;

    private AggCallNameSetter(RelBuilder relBuilder) {
      this.relBuilder = relBuilder;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (!(other instanceof AggregateRel)) {
        return super.visit(other);
      }

      AggregateRel aggregateRel = (AggregateRel) other;

      final int[] counter = {0};
      List<AggregateCall> rewrittenAggregateCalls = aggregateRel
        .getAggCallList()
        .stream()
        .map(aggregateCall -> {
          if (aggregateCall.getName() != null) {
            return aggregateCall;
          }

          String name = "aggCall" + counter[0];
          counter[0] = counter[0] + 1;
          AggregateCall newAggCall = AggregateCall.create(
            aggregateCall.getAggregation(),
            aggregateCall.isDistinct(),
            aggregateCall.isApproximate(),
            aggregateCall.getArgList(),
            aggregateCall.filterArg,
            aggregateCall.getCollation(),
            aggregateCall.getType(),
            name);

          return newAggCall;
        })
        .collect(Collectors.toList());

      return relBuilder
        .push(aggregateRel.getInput())
        .aggregate(relBuilder.groupKey(aggregateRel.getGroupSet()), rewrittenAggregateCalls)
        .build();
    }
  }

  private static final class FlattenInputReplacer extends StatelessRelShuttleImpl {
    private final RelNode table;
    private final RelBuilder relBuilder;
    private final Map<RelNode, Set<RexFieldAccess>> correlateVariableMapping;

    private FlattenInputReplacer(
      RelNode table,
      RelBuilder relBuilder,
      Map<RelNode, Set<RexFieldAccess>> correlateVariableMapping) {
      this.table = table;
      this.relBuilder = relBuilder;
      this.correlateVariableMapping = correlateVariableMapping;
    }

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof HepRelVertex) {
        HepRelVertex hepRelVertex = (HepRelVertex) other;
        return hepRelVertex.getCurrentRel().accept(this);
      } else if (other instanceof AggregateRel) {
        return visitAggregate((AggregateRel) other);
      } else if (other instanceof ProjectRel) {
        return visitProject((ProjectRel) other);
      } else if (other instanceof JoinRel) {
        return visitJoin((JoinRel) other);
      } else if (other instanceof FilterRel) {
        return visitFilter((FilterRel) other);
      } else if (other instanceof FlattenRel) {
        return visitFlatten((FlattenRel) other);
      } else if (other instanceof ValuesRel) {
        return other;
      } else {
        throw new UnsupportedOperationException("Can not decorrelate an UNNEST query with : " + other.getRelTypeName());
      }
    }

    private RelNode visitAggregate(AggregateRel aggregate) {
      boolean hasListAgg = aggregate
        .getAggCallList()
        .stream()
        .anyMatch(aggregateCall -> aggregateCall.getAggregation() == ARRAY_AGG);
      if (hasListAgg) {
        throw UserException
          .planError()
          .message("Cannot group by an array and aggregate with ARRAY_AGG at the same time.")
          .buildSilently();
      }

      // We need to add the correlate variables to the group set, since it get used in the join key
      RelNode rewrittenInput = aggregate.getInput().accept(this);
      if (rewrittenInput.equals(aggregate.getInput())) {
        return aggregate;
      }

      RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      RexInputRefMapper mapper = RexInputRefMapper.create(rexBuilder, aggregate.getInput(), rewrittenInput);

      Set<RexFieldAccess> correlatedVariables = correlateVariableMapping.get(aggregate);
      List<RexNode> inputRefsForGrouping = new ArrayList<>();
      for (RexFieldAccess rexFieldAccess : correlatedVariables) {
        RexNode inputRefForGrouping = rexFieldAccess.accept(mapper);
        inputRefsForGrouping.add(inputRefForGrouping);
      }

      for (int index : aggregate.getGroupSet().asList()) {
        RexNode inputRefForGrouping = rexBuilder.makeInputRef(aggregate.getInput(), index);
        inputRefForGrouping = inputRefForGrouping.accept(mapper);
        inputRefsForGrouping.add(inputRefForGrouping);
      }

      List<AggregateCall> rewrittenAggregateCalls = aggregate
        .getAggCallList()
        .stream()
        .map(aggregateCall -> {
          List<Integer> newArgs = aggregateCall
            .getArgList()
            .stream()
            .map(x -> ((RexInputRef) rexBuilder
              .makeInputRef(aggregate.getInput(), x)
              .accept(mapper))
              .getIndex())
            .collect(Collectors.toList());
          int newFilter = -1;
          if (aggregateCall.filterArg != -1) {
            newFilter = ((RexInputRef) rexBuilder
              .makeInputRef(aggregate.getInput(), aggregateCall.filterArg)
              .accept(mapper))
              .getIndex();
          }

          RelCollation newCollation = RelCollations.of(aggregateCall
            .getCollation()
            .getFieldCollations()
            .stream()
            .map(relFieldCollation -> {
              RexInputRef inputRef = rexBuilder.makeInputRef(aggregate.getInput(), relFieldCollation.getFieldIndex());
              RexInputRef newInputRef = (RexInputRef) inputRef.accept(mapper);
              return relFieldCollation.copy(newInputRef.getIndex());
            }).collect(Collectors.toList()));
          return aggregateCall.copy(newArgs, newFilter, newCollation);
        })
        .collect(Collectors.toList());

      return relBuilder
        .push(rewrittenInput)
        .aggregate(relBuilder.groupKey(inputRefsForGrouping), rewrittenAggregateCalls)
        .build();
    }

    private RelNode visitProject(ProjectRel project) {
      // We need to insert a column for the join key and bump all the ref indexes.
      RelNode rewrittenInput = project.getInput().accept(this);

      RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      RexInputRefMapper mapper = RexInputRefMapper.create(rexBuilder, project.getInput(), rewrittenInput);
      Set<RexFieldAccess> correlatedVariables = correlateVariableMapping.get(project);

      // We need to preserve the join key + correlate variables introduced in uncollect.
      List<RexNode> newProjects = new ArrayList<>();
      for (RexFieldAccess rexFieldAccess : correlatedVariables) {
        RexNode newProject = rexFieldAccess.accept(mapper);
        newProjects.add(newProject);
      }

      for (RexNode projection : project.getProjects()) {
        RexNode adujustedProjection = projection.accept(mapper);
        newProjects.add(adujustedProjection);
      }

      List<String> newAliases = new ArrayList<>();
      for (RexFieldAccess rexFieldAccess : correlatedVariables) {
        newAliases.add(rexFieldAccess.getField().getName());
      }

      newAliases.addAll(project.getRowType().getFieldNames());

      RelNode rewrittenProject = relBuilder
        .push(rewrittenInput)
        .project(newProjects, newAliases, true)
        .build();

      return rewrittenProject;
    }

    private RelNode visitJoin(JoinRel join) {
      // We need to update the input refs in the condition to align with the fact that we are introducing a join key
      JoinRel rewrittenJoin = (JoinRel) super.visit(join);
      RelNode rewrittenLeft = rewrittenJoin.getLeft();
      RelNode rewrittenRight = rewrittenJoin.getRight();

      RexNode rewrittenJoinCondition = RexInputRefMapper.applyMapping(
        join.getCondition(),
        join,
        rewrittenJoin);

      rewrittenJoin = (JoinRel) relBuilder
        .push(rewrittenLeft)
        .push(rewrittenRight)
        .join(join.getJoinType(), rewrittenJoinCondition)
        .build();

      return rewrittenJoin;
    }

    private RelNode visitFilter(FilterRel filter) {
      // We need to adjust all the ref index to account for the join key.
      RelNode rewrittenInput = filter.getInput().accept(this);

      RexNode rewrittenCondition = RexInputRefMapper.applyMapping(
        filter.getCondition(),
        filter.getInput(),
        rewrittenInput);

      return relBuilder
        .push(rewrittenInput)
        .filter(rewrittenCondition)
        .build();
    }

    private RelNode visitFlatten(FlattenRel flatten) {
      Set<RexFieldAccess> correlatedVariables = correlateVariableMapping.get(flatten);
      if (correlatedVariables.isEmpty()) {
        // We don't need to do any decorrelation here:
        return flatten;
      }

      if (correlatedVariables.size() > 1) {
        throw UserException.planError()
          .message("We only support one correlated variable under an UNNEST.")
          .buildSilently();
      }

      List<RexInputRef> flattenIndices = flatten.getToFlatten();
      if (flattenIndices.size() > 1) {
        throw UserException.planError()
          .message("Expected flatten to only have a single flatten field")
          .buildSilently();
      }

      Project input = (Project) flatten.getInput();
      int flattenIndex = flattenIndices.get(0).getIndex();
      RexNode flattenExpr = input.getProjects().get(flattenIndex);
      if (!(flattenExpr instanceof RexFieldAccess)) {
        // The correlated variable is not under this flatten, so recurse:
        RelNode rewrittenInput = flatten.getInput().accept(this);
        RexInputRef newToFlatten = (RexInputRef) RexInputRefMapper.applyMapping(
          flatten.getToFlatten().get(0),
          flatten.getInput(),
          rewrittenInput);

        FlattenRel rewrittenFlatten = FlattenRel.create(
          rewrittenInput,
          newToFlatten.getIndex(),
          flatten.getAliases().get(0));
        return rewrittenFlatten;
      }

      RexFieldAccess flattenCorrelatedVariable = (RexFieldAccess) flattenExpr;
      /*
       * We need to:
       *  1) Decorrelate the Flatten call
       *  2) Preserve the ARRAY we are trying to flatten, so that we can later use it as a JOIN key
       *      Flatten
       *        Project(array_to_uncollect=[$corX.column_name])
       *          Values(tuples=[[{ 0 }]])
       *
       *      Flatten($1)
       *        Project($index_matching_the_correlated_column, $index_matching_the_correlated_column)
       *          TableScan(myTable)
       */

      List<RexInputRef> decorrelatedVariables = new ArrayList<>();
      RexBuilder rexBuilder = flatten.getCluster().getRexBuilder();
      for (RexFieldAccess correlatedVariable : correlatedVariables) {
        RexInputRef decorrelatedVariable = rexBuilder.makeInputRef(table, correlatedVariable.getField().getIndex());
        decorrelatedVariables.add(decorrelatedVariable);
      }

      // We need to take only the distinct arrays, since we don't want to join on duplicates
      RelNode distinctArrays = relBuilder
        .push(table)
        .aggregate(relBuilder.groupKey(decorrelatedVariables))
        .build();

      // We have to project the field we want to flatten TWICE
      // if we want to preserve both the array and the item,
      // since it gets lost in the FLATTEN call
      // We also need to add all the correlated variables that are referenced in the upper RelNodes.
      RexNode firstInputRef = rexBuilder.makeInputRef(distinctArrays, 0);

      List<RexNode> doubleProjectNodes = new ArrayList<>();
      doubleProjectNodes.add(firstInputRef);
      doubleProjectNodes.add(firstInputRef);
      for (int i = 1; i < distinctArrays.getRowType().getFieldCount(); i++) {
        RexNode passthroughInputRef = rexBuilder.makeInputRef(distinctArrays, i);
        doubleProjectNodes.add(passthroughInputRef);
      }

      List<String> doubleProjectAliases = new ArrayList<>();
      String flattenFieldName = flattenCorrelatedVariable.getField().getName();
      doubleProjectAliases.add(flattenFieldName);
      String flattenAlias = flatten.getRowType().getFieldList().get(0).getName();
      doubleProjectAliases.add(flattenAlias);

      RelNode doubleProject = relBuilder
        .push(distinctArrays)
        .project(doubleProjectNodes, doubleProjectAliases)
        .build();

      FlattenRel rewrittenFlatten = FlattenRel.create(
        doubleProject,
        1,
        flattenAlias);
      return rewrittenFlatten;
    }

    private static final class RexInputRefMapper extends RexShuttle {
      private final RexBuilder rexBuilder;
      private final List<RelDataTypeField> originalFields;
      private final List<RelDataTypeField> newFields;

      private RexInputRefMapper(
        RexBuilder rexBuilder,
        List<RelDataTypeField> originalFields,
        List<RelDataTypeField> newFields) {
        this.rexBuilder = rexBuilder;
        this.originalFields = originalFields;
        this.newFields = newFields;
      }

      @Override
      public RexNode visitInputRef(RexInputRef rexInputRef) {
        int originalIndex = rexInputRef.getIndex();
        RelDataTypeField originalField = originalFields.get(originalIndex);
        for (int newIndex = originalIndex; newIndex < newFields.size(); newIndex++) {
          RelDataTypeField newField = newFields.get(newIndex);
          if (originalField.getName().equals(newField.getName())) {
            return rexBuilder.makeInputRef(newField.getType(), newIndex);
          }
        }

        throw new RuntimeException("Failed to find the matching rex input ref.");
      }

      @Override
      public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
        RexNode referenceExpr = fieldAccess.getReferenceExpr();
        if (!(referenceExpr instanceof RexCorrelVariable)) {
          return super.visitFieldAccess(fieldAccess);
        }

        for (int index = 0; index < newFields.size(); index++) {
          RelDataTypeField newField = newFields.get(index);
          if (fieldAccess.getField().getName().equals(newField.getName())) {
            return rexBuilder.makeInputRef(newField.getType(), index);
          }
        }

        throw new RuntimeException("Failed to replace correlate variable.");
      }

      public static RexInputRefMapper create(
        RexBuilder rexBuilder,
        RelNode originalInput,
        RelNode rewrittenInput) {
        return RexInputRefMapper.create(
          rexBuilder,
          Collections.singletonList(originalInput),
          Collections.singletonList(rewrittenInput));
      }

      public static RexNode applyMapping(
        RexNode node,
        RelNode originalInput,
        RelNode rewrittenInput) {
        return node.accept(create(originalInput.getCluster().getRexBuilder(), originalInput, rewrittenInput));
      }

      private static RexInputRefMapper create(
        RexBuilder rexBuilder,
        List<RelNode> originalInputs,
        List<RelNode> rewrittenInputs) {
        List<RelDataTypeField> originalFields = new ArrayList<>();
        for (RelNode originalInput : originalInputs) {
          originalFields.addAll(originalInput.getRowType().getFieldList());
        }

        List<RelDataTypeField> rewrittenFields = new ArrayList<>();
        for (RelNode rewrittenInput : rewrittenInputs) {
          rewrittenFields.addAll(rewrittenInput.getRowType().getFieldList());
        }

        return new RexInputRefMapper(rexBuilder, originalFields, rewrittenFields);
      }
    }
  }

  private static boolean hasFlatten(RelNode relNode) {
    return hasProperty(relNode, node -> node instanceof FlattenRel);
  }

  private static boolean hasCorrelate(RelNode relNode) {
    return hasProperty(relNode, node -> node instanceof Correlate);
  }

  private static boolean hasProperty(RelNode relNode, Function<RelNode, Boolean> predicate) {
    if (predicate.apply(relNode)) {
      return true;
    }

    if (relNode instanceof HepRelVertex) {
      HepRelVertex hepRelVertex = (HepRelVertex) relNode;
      return hasProperty(hepRelVertex.getCurrentRel(), predicate);
    }

    for (RelNode input : relNode.getInputs()) {
      if (hasProperty(input, predicate)) {
        return true;
      }
    }

    return false;
  }
}
