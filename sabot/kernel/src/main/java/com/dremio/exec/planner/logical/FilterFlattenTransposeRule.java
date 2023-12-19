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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Push filter past flatten (as possible). May split flatten into multiple operators as necessary.
 */
public class FilterFlattenTransposeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new FilterFlattenTransposeRule();

  private FilterFlattenTransposeRule() {
    super(RelOptHelper.any(FilterRel.class, FlattenRel.class), "PushFilterPastFlattenRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final FilterRel filter = call.rel(0);
    return !filter.isAlreadyPushedDown();
  }

  private static class PushConjunct {

    private final Set<Integer> requiredFlattens;
    private final RexNode condition;

    public PushConjunct(RexNode condition,
        Set<Integer> requiredFlattens) {
      super();

      this.requiredFlattens = requiredFlattens;
      this.condition = condition;
    }

    public static PushConjunct merge(RexBuilder rexBuilder, Set<PushConjunct> conjuncts){

      Set<Integer> totalRequired = FluentIterable.from(conjuncts).transformAndConcat(new Function<PushConjunct,Set<Integer>>(){
        @Override
        public Set<Integer> apply(PushConjunct input) {
          return input.requiredFlattens;
        }}).toSet();

      List<RexNode> nodes = FluentIterable.from(conjuncts).transform(new Function<PushConjunct, RexNode>(){

        @Override
        public RexNode apply(PushConjunct input) {
          return input.condition;
        }}).toList();

      RexNode condition = RexUtil.composeConjunction(rexBuilder, nodes, false);

      return new PushConjunct(condition, totalRequired);
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRel filter = call.rel(0);
    final FlattenRel flatten = call.rel(1);
    final RelNode child = flatten.getInput();

    // For each input, generate a set of conjuctions that rely on that input.
    // Then generate an alternative plan that moves all other conjunctions below the flatten
    Set<Integer> flattenedFields = flatten.getFlattenedIndices();
    List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
    List<RexNode> alwaysPushable = new ArrayList<>();
    List<RexNode> neverPushable = new ArrayList<>();

    final HashMultimap<Integer, PushConjunct> pushOptions = HashMultimap.create();
    final Set<PushConjunct> allConjuncts = new HashSet<>();

    for(RexNode c : conjuncts){
      final InputFinder f = new InputFinder();
      c.accept(f);
      Set<Integer> requiredFlattens = Sets.intersection(f.inputs, flattenedFields);
      if(requiredFlattens.isEmpty()){
        alwaysPushable.add(c);
      }else if(requiredFlattens.size() == flattenedFields.size()){
        neverPushable.add(c);
      }else{
        // this can't always be pushed down, record when it can.
        PushConjunct conjunct = new PushConjunct(c, requiredFlattens);
        allConjuncts.add(conjunct);
        for(Integer i : requiredFlattens){
          pushOptions.put(i, conjunct);
        }
      }
    }

    final RexNode neverPushableCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), neverPushable, false);
    final RexNode alwaysPushableCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), alwaysPushable, false);

    // for each flatten, take always pushable and then add each conjunct that is not in the dependent set and push those.
    for(Integer i : flattenedFields){
      Set<PushConjunct> dependencies = pushOptions.get(i);
      if(dependencies == null){
        dependencies = ImmutableSet.<PushConjunct>of();
      }
      Set<PushConjunct> pushAvailable = Sets.difference(allConjuncts, dependencies);
      if(!pushAvailable.isEmpty()){
        // impossible to push this filter past the given flatten.
        continue;
      }

      // split the current flatten into two flattens (at most)
      // and push the filter condition below the upper flatten.
      // The upper one includes the required field. The lower one includes all other fields.
      // the filter includes the merged conjunctions of the condition pushed and all
      PushConjunct merged = PushConjunct.merge(filter.getCluster().getRexBuilder(), pushAvailable);
      RexNode fullPushCondition = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), ImmutableList.<RexNode>of(alwaysPushableCondition, merged.condition), false);

      if(fullPushCondition.isAlwaysTrue()){
        // we can't push anything.
        continue;
      }

      //lower flatten.
      Set<Integer> aboveFlattenFields = ImmutableSet.of(i);
      Set<Integer> belowFlattenFields = Sets.difference(flattenedFields, aboveFlattenFields);
      RelNode newInput = belowFlattenFields.isEmpty() ? child : new FlattenRel(filter.getCluster(), flatten.getTraitSet(), child, asNodes(filter.getCluster(), belowFlattenFields), flatten.getAliases(), flatten.getNumProjectsPushed());
      RelNode newFilter = new FilterRel(filter.getCluster(), filter.getTraitSet(), newInput, fullPushCondition);
      RelNode topFlatten = new FlattenRel(filter.getCluster(), filter.getTraitSet(), newFilter, asNodes(filter.getCluster(), aboveFlattenFields), flatten.getAliases(), flatten.getNumProjectsPushed());
      RelNode top = neverPushableCondition.isAlwaysTrue() ? topFlatten : new FilterRel(filter.getCluster(), filter.getTraitSet(), topFlatten, neverPushableCondition);
      call.transformTo(top);
    }
  }

  private static List<RexInputRef> asNodes(final RelOptCluster cluster, Iterable<Integer> indices){
    final RexBuilder builder = cluster.getRexBuilder();
    final RelDataTypeFactory factory = cluster.getTypeFactory();

    return FluentIterable.from(indices).transform(new Function<Integer, RexInputRef>(){
      @Override
      public RexInputRef apply(Integer input) {
        return builder.makeInputRef(factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true), input);
      }}).toList();
  }

  private class InputFinder extends RexShuttle {

    private final Set<Integer> inputs = new HashSet<>();

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      inputs.add(inputRef.getIndex());
      return super.visitInputRef(inputRef);
    }

  }

}
