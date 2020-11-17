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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiShuttle;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.calcite.logical.FlattenCrel;
import com.dremio.exec.planner.sql.SqlFlattenOperator;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;

/**
 * A rule that canonicalizes flatten expressions by their flatten index. It then
 * creates a tree of LogicalProjects and FlattenCrels, ensuring that all
 * SqlFlattenOperator expressions are removed. The output is guaranteed to not
 * have any flattens embedded in projects and is also guaranteed to only have a
 * single flatten operation for each distinct SqlFlattenOperator index.
 */
public class RewriteProjectToFlattenRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new RewriteProjectToFlattenRule(LogicalProject.class);

  private RewriteProjectToFlattenRule(Class<? extends Project> projectClass) {
    super(RelOptHelper.any(projectClass), "RewriteProjectToFlattenRule_" + projectClass.getName());
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Project project = call.rel(0);
    return FlattenVisitors.hasFlatten(project);
  }

  /**
   * Our goal here is do
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);

    final ExpressionAnalyzer expressionBuilder = new ExpressionAnalyzer();
    expressionBuilder.analyze(call.<Project>rel(0));

    final int highestLevel = expressionBuilder.maxLevel;

    RelNode input = project.getInput();
    for(int level = 0; level <= highestLevel; level++){

      List<ProjectSlotHolder> projHolders = expressionBuilder.projectLevels.get(level);

      List<String> fieldNames = new ArrayList<>();
      List<RexNode> projectExpressions = new ArrayList<>();
      for(int i = 0; i < projHolders.size(); i++) {
        ProjectSlotHolder e = projHolders.get(i);
        // write the final slot location of this data.
        e.outputReference.index = i;
        fieldNames.add(String.format("expr%03d", i));
        projectExpressions.add(e.expression.accept(new RemoveMutableRexInputRef()));
      }

      RelBuilder relBuilder = relBuilderFactory.create(project.getCluster(), null);
      relBuilder.push(input);
      relBuilder.project(projectExpressions, fieldNames);
      input = relBuilder.build();

      List<FlattenExpression> flattenHolders = expressionBuilder.flattenLevels.get(level);

      if(flattenHolders != null && flattenHolders.size() > 0){

        List<RexInputRef> flattenExpressions = new ArrayList<>();
        for(FlattenExpression e : flattenHolders) {
          flattenExpressions.add((RexInputRef) e.inputReference.accept(new RemoveMutableRexInputRef()));
        }

        input = new FlattenCrel(
            project.getCluster(),
            project.getTraitSet(),
            input,
            flattenExpressions,
            0);
      }
    }

    // add top level project.
    RelBuilder relBuilder = relBuilderFactory.create(project.getCluster(), null);
    relBuilder.push(input);
    List<RexNode> finalizedExpressions = expressionBuilder.topProjectExpressions.stream()
      .map(input1 -> input1.accept(new RemoveMutableRexInputRef())).collect(Collectors.toList());

    relBuilder.project(finalizedExpressions, project.getRowType().getFieldNames());
    input = relBuilder.build();

    call.transformTo(input);
  }

  private class ProjectSlotHolder {
    private final RexNode expression;
    private final MutableRexInputRef outputReference;

    public ProjectSlotHolder(RexNode expression, MutableRexInputRef outputReference) {
      super();
      this.expression = expression;
      this.outputReference = outputReference;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("expression", expression).add("outputReference", outputReference)
          .toString();
    }

  }

  private static class FlattenExpression {
    private final int level;
    private final MutableRexInputRef inputReference;

    FlattenExpression(int level, MutableRexInputRef inputReference) {
      super();
      this.level = level;
      this.inputReference = inputReference;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("level", level).add("inputReference", inputReference).toString();
    }

  }

  /**
   * A specialized RexInputRef that allows us to change the pointer after
   * initial construction. Allows us to build an expression tree once and later
   * decide what input it points to. It is only used to mutate once we are
   * constructing the tree, thus the reason it is a private class. Before we
   * return a RexNode tree, we replace mutable refs with the immutable variant.
   */
  private static class MutableRexInputRef extends RexInputRef {

    private Integer index;

    public MutableRexInputRef(RelDataType type) {
      super(0, type);
    }

    @Override
    public int getIndex(){
      Preconditions.checkNotNull(index, "Mutable index must be set before retrieval.");
      return index;
    }

    @Override
    public boolean equals(final Object other) {
      // since mutable rexinput refs are based on references, not indices (since they may not be defined), we need to only support identity equality.
      return other == this;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(index);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("index", index).add("hc", System.identityHashCode(this)).toString();
    }



  }

  /**
   * A pointer to a integer with a special names. Used to have a second return
   * value from the RelShuttle.
   */
  private static final class LevelHolder {
    private int index = 0;

    LevelHolder(){
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("index", index).toString();
    }

  }

  private class RemoveMutableRexInputRef extends RexShuttle {

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      if(inputRef instanceof MutableRexInputRef){
        return new RexInputRef(inputRef.getIndex(), inputRef.getType());
      }
      return super.visitInputRef(inputRef);
    }

  }

  /**
   * Replaces input refs with MutableRexInptuRefs so that we can propagate references down tree.
   */
  private class InputRefReplacerAnalyzer extends RexShuttle {

    private List<ProjectSlotHolder> holders = new ArrayList<>();
    private Map<Integer, MutableRexInputRef> refMap = new HashMap<>();
    private Set<MutableRexInputRef> refs;
    private RexBuilder rexBuilder;

    private InputRefReplacerAnalyzer(List<ProjectSlotHolder> children, RexBuilder rexBuilder) {
      this.refs = children.stream().map(input -> input.outputReference).collect(Collectors.toSet());
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {

      // see if we have a reference to our child.
      if(inputRef instanceof MutableRexInputRef && refs.contains(inputRef)) {
        return inputRef;
      }

      if(!(inputRef instanceof MutableRexInputRef)) {
        MutableRexInputRef previousPointer = refMap.get(inputRef.getIndex());
        if(previousPointer != null){
          return previousPointer;
        }
      }

      // create a new holder to add to the child of this.
      RelDataType type = inputRef.getType().getComponentType() == null ? inputRef.getType() : inputRef.getType().getComponentType();
      final MutableRexInputRef inputPointer = new MutableRexInputRef(type);
      holders.add(new ProjectSlotHolder(inputRef, inputPointer));

      if( !(inputRef instanceof MutableRexInputRef) ){
        refMap.put(inputRef.getIndex(), inputPointer);
      }
      return inputPointer;
    }
  }

  /**
   * Breaks flattens and other expressions into levels.
   */
  private class ExpressionAnalyzer extends RexBiShuttle<LevelHolder> {

    private Map<Integer, FlattenExpression> flattenOutputs = new HashMap<>();
    private ListMultimap<Integer, ProjectSlotHolder> projectLevels = ArrayListMultimap.create();
    private ListMultimap<Integer, FlattenExpression> flattenLevels = ArrayListMultimap.create();
    private List<RexNode> topProjectExpressions;

    private int maxLevel = 0;

    public void analyze(Project project) {
      final LevelHolder highestLevelHolder = new LevelHolder();
      final ExpressionAnalyzer analyzer = this;

      final List<RexNode> finalExpressions = project.getChildExps().stream().map(input -> {
        LevelHolder level = new LevelHolder();
        RexNode output = input.accept(analyzer, level);
        highestLevelHolder.index = Math.max(level.index, highestLevelHolder.index);
        return output;
      }).collect(Collectors.toList());


      // the maxLevel is one less than reported because we report the level we need to start.
      this.maxLevel = highestLevelHolder.index - 1;

      // now add the final expressions to a top level project.
      {
        List<RexNode> topProject = new ArrayList<>();
        int child = maxLevel;

        List<ProjectSlotHolder> children = new ArrayList<>();
        children.addAll(projectLevels.get(child));

        FindNonDependent dep = new FindNonDependent(children.stream().map(input -> input.outputReference).collect(Collectors.toSet()));
        InputRefReplacerAnalyzer downProp = new InputRefReplacerAnalyzer(children, project.getCluster().getRexBuilder());
        for(RexNode e : finalExpressions) {
          if(e.accept(dep)){
            // this doesn't need the top flatten, push it down.
            final MutableRexInputRef inputPointer = new MutableRexInputRef(e.getType());
            ProjectSlotHolder slot = new ProjectSlotHolder(e, inputPointer);
            topProject.add(inputPointer);
            children.add(slot);
          } else {
            topProject.add(e.accept(downProp));
          }
        }

        // add missing expressions for top level project.
        for(ProjectSlotHolder holder : downProp.holders){
          children.add(holder);
        }

        projectLevels.replaceValues(child, children);
        this.topProjectExpressions = topProject;
      }

      // now propagate the rest of expression trees down.
      for(int i = maxLevel; i > 0; i--){
        int child = i - 1;

        List<ProjectSlotHolder> holders = projectLevels.get(i);
        List<ProjectSlotHolder> updatedHolders = new ArrayList<>();
        List<ProjectSlotHolder> children = new ArrayList<>();
        children.addAll(projectLevels.get(child));
        FindNonDependent dep = new FindNonDependent(children.stream().map(input -> input.outputReference).collect(Collectors.toSet()));

        InputRefReplacerAnalyzer downProp = new InputRefReplacerAnalyzer(children, project.getCluster().getRexBuilder());
        for(ProjectSlotHolder e : holders){
          if(e.expression.accept(dep)){
            // this doesn't need the top flatten, push it down.
            final MutableRexInputRef inputPointer = new MutableRexInputRef(e.expression.getType());
            updatedHolders.add(new ProjectSlotHolder(inputPointer, e.outputReference));
            children.add(new ProjectSlotHolder(e.expression, inputPointer));
          } else {
            // requires child, simply propagate
            RexNode newExpression = e.expression.accept(downProp);
            updatedHolders.add(new ProjectSlotHolder(newExpression, e.outputReference));
          }
        }

        // add missing expressions;
        for(ProjectSlotHolder holder : downProp.holders){
          children.add(holder);
        }

        projectLevels.replaceValues(i, updatedHolders);
        projectLevels.replaceValues(child, children);
      }

    }

    @Override
    protected List<RexNode> visitList(
        List<? extends RexNode> exprs, boolean[] update, LevelHolder output) {
      ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
      int level = 0;
      for (RexNode operand : exprs) {
        LevelHolder cnt = new LevelHolder();
        RexNode clonedOperand = operand.accept(this, cnt);
        if ((clonedOperand != operand) && (update != null)) {
          update[0] = true;
        }
        level = Math.max(cnt.index, level);
        clonedOperands.add(clonedOperand);
      }
      output.index = level;
      return clonedOperands.build();
    }

    @Override
    public RexNode visitCall(RexCall function, LevelHolder outgoingLevel) {
      if (!(function.getOperator() instanceof SqlFlattenOperator)) {
        return super.visitCall(function, outgoingLevel);
      }


      final int index = ((SqlFlattenOperator) function.getOperator()).getIndex();

      FlattenExpression flattenOutput = flattenOutputs.get(index);

      if(flattenOutput != null){
        outgoingLevel.index = flattenOutput.level + 1;
        return flattenOutput.inputReference;

      } else {
        Preconditions.checkArgument(function.getOperands().size() == 1, "Flatten only supports a single operand.");

        LevelHolder inputLevel = new LevelHolder();
        RexNode newRexInput = function.getOperands().get(0).accept(this, inputLevel);

        RelDataType type = newRexInput.getType().getComponentType() == null ? newRexInput.getType() : newRexInput.getType().getComponentType();
        final MutableRexInputRef inputPointer = new MutableRexInputRef(type);
        projectLevels.put(inputLevel.index, new ProjectSlotHolder(newRexInput, inputPointer));


        final FlattenExpression flatten = new FlattenExpression(inputLevel.index, inputPointer);
        flattenOutputs.put(index, flatten);
        flattenLevels.put(inputLevel.index, flatten);

        // the output level will be one more than the input.
        outgoingLevel.index = inputLevel.index + 1;
        return inputPointer;
      }

    }

  }

  private static class FindNonDependent implements RexVisitor<Boolean> {

    private final Set<RexNode> children;

    public FindNonDependent(Set<RexNode> children) {
      super();
      this.children = children;
    }

    @Override
    public Boolean visitInputRef(RexInputRef inputRef) {
      return !children.contains(inputRef);
    }

    @Override
    public Boolean visitLocalRef(RexLocalRef localRef) {
      return true;
    }

    @Override
    public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return true;
    }

    @Override
    public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
      return true;
    }

    @Override
    public Boolean visitLiteral(RexLiteral literal) {
      return true;
    }

    @Override
    public Boolean visitCall(RexCall call) {
      return list(call.getOperands());
    }

    private boolean list(Iterable<RexNode> nodes){
      boolean out = true;
      for(RexNode n : nodes){
        out = out && n.accept(this);
      }
      return out;
    }

    @Override
    public Boolean visitOver(RexOver over) {
      return list(over.getOperands());
    }

    @Override
    public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return true;
    }

    @Override
    public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return true;
    }

    @Override
    public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return true;
    }

    @Override
    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      return fieldAccess.getReferenceExpr().accept(this);
    }

    @Override
    public Boolean visitSubQuery(RexSubQuery subQuery) {
      return true;
    }

  }

}
