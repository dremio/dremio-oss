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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET;

import com.dremio.common.JSONOptions;
import com.dremio.common.logical.PlanProperties;
import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.logical.PlanProperties.PlanPropertiesBuilder;
import com.dremio.common.logical.PlanProperties.PlanType;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.visitor.BridgeReaderSchemaFinder;
import com.dremio.exec.planner.physical.visitor.CSEIdentifier;
import com.dremio.exec.planner.physical.visitor.ComplexToJsonPrelVisitor;
import com.dremio.exec.planner.physical.visitor.EmptyPrelPropagator;
import com.dremio.exec.planner.physical.visitor.ExcessiveExchangeIdentifier;
import com.dremio.exec.planner.physical.visitor.ExpandNestedFunctionVisitor;
import com.dremio.exec.planner.physical.visitor.FinalColumnReorderer;
import com.dremio.exec.planner.physical.visitor.InsertHashProjectVisitor;
import com.dremio.exec.planner.physical.visitor.InsertLocalExchangeVisitor;
import com.dremio.exec.planner.physical.visitor.JoinConditionValidatorVisitor;
import com.dremio.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import com.dremio.exec.planner.physical.visitor.RelUniqifier;
import com.dremio.exec.planner.physical.visitor.RuntimeFilterDecorator;
import com.dremio.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import com.dremio.exec.planner.physical.visitor.SimpleLimitExchangeRemover;
import com.dremio.exec.planner.physical.visitor.SplitCountChecker;
import com.dremio.exec.planner.physical.visitor.SplitUpComplexExpressions;
import com.dremio.exec.planner.physical.visitor.StarColumnConverter;
import com.dremio.exec.planner.physical.visitor.SwapHashJoinVisitor;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.fromjson.ComplexFunctionPushDownVisitor;
import com.dremio.sabot.op.fromjson.ConvertFromJsonConverter;
import com.dremio.sabot.op.fromjson.ConvertFromUnnester;
import com.dremio.sabot.op.join.JoinUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Pair;

/** Collection of Rel, Drel and Prel transformations used in various planning cycles. */
public final class PrelTransformer {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(PrelTransformer.class);

  private PrelTransformer() {}

  public static Pair<Prel, String> convertToPrel(SqlHandlerConfig config, RelNode drel)
      throws RelConversionException, SqlUnsupportedException {
    Preconditions.checkArgument(drel.getConvention() == Rel.LOGICAL);

    final RelTraitSet traits =
        drel.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON);
    Prel phyRelNode;
    try {
      final Stopwatch watch = Stopwatch.createStarted();
      final RelNode prel1 =
          PlannerUtil.transform(
              config, PlannerType.VOLCANO, PlannerPhase.PHYSICAL, drel, traits, true);

      final RelNode prel2 =
          PlannerUtil.transform(
              config,
              PlannerType.HEP_AC,
              PlannerPhase.PHYSICAL_HEP,
              prel1,
              prel1.getTraitSet(),
              true);
      phyRelNode = (Prel) prel2.accept(new PrelFinalizer());
      if (config.getContext().getWorkloadType() == UserBitShared.WorkloadType.ACCELERATOR) {
        phyRelNode = (Prel) phyRelNode.accept(new IncrementalRefreshByPartitionFinalizeShuttle());
      }
      // log externally as we need to finalize before traversing the tree.
      PlanLogUtil.log(PlannerType.VOLCANO, PlannerPhase.PHYSICAL, phyRelNode, logger, watch);
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage());

      if (JoinUtils.checkCartesianJoin(
          drel, new ArrayList<>(), new ArrayList<>(), Lists.<Boolean>newArrayList())) {
        throw new UnsupportedRelOperatorException(
            "This query cannot be planned\u2014possibly due to use of an unsupported feature.");
      } else {
        throw ex;
      }
    }
    return applyPhysicalPrelTransformations(config, phyRelNode);
  }

  public static Pair<Prel, String> applyPhysicalPrelTransformations(
      SqlHandlerConfig config, Prel phyRelNode) throws RelConversionException {
    QueryContext context = config.getContext();
    OptionManager queryOptions = context.getOptions();
    final PlannerSettings plannerSettings = context.getPlannerSettings();

    /*
     * Convert AND with not equal expressions to NOT-OR with equal conditions
     * to make query use InExpression logical expression
     */
    phyRelNode = (Prel) phyRelNode.accept(new AndToOrConverter());

    /* Disable distribution trait pulling
     *
     * Some of the following operations might rewrite the tree but would not
     * keep distribution traits consistent anymore (like ExcessiveExchangeIdentifier
     * which would remove hash distribution exchanges if no parallelization will occur).
     */
    plannerSettings.pullDistributionTrait(false);

    /*
     * Check whether the query is within the required number-of-splits limit(s)
     */
    phyRelNode =
        SplitCountChecker.checkNumSplits(
            phyRelNode,
            plannerSettings.getQueryMaxSplitLimit(),
            plannerSettings.getDatasetMaxSplitLimit());

    /* The order of the following transformations is important */
    final Stopwatch finalPrelTimer = Stopwatch.createStarted();

    /*
     * 0.) For select * from join query, we need insert project on top of scan and a top project just
     * under screen operator. The project on top of scan will rename from * to T1*, while the top project
     * will rename T1* to *, before it output the final result. Only the top project will allow
     * duplicate columns, since user could "explicitly" ask for duplicate columns ( select *, col, *).
     * The rest of projects will remove the duplicate column when we generate POP in json format.
     */
    phyRelNode = StarColumnConverter.insertRenameProject(phyRelNode);

    /*
     * 1.1)
     * Join might cause naming conflicts from its left and right child.
     * In such case, we have to insert Project to rename the conflicting names.
     */
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);

    /*
     * 1.2.) Swap left / right for INNER hash join, if left's row count is < (1 + margin) right's row count.
     * We want to have smaller dataset on the right side, since hash table builds on right side.
     */
    if (plannerSettings.isHashJoinSwapEnabled()) {
      phyRelNode =
          SwapHashJoinVisitor.swapHashJoin(
              phyRelNode, plannerSettings.getHashJoinSwapMarginFactor());
    }

    /*
     * 1.3.a) Unnest all the CONVERT_FROMJSON calls
     */
    phyRelNode = ConvertFromUnnester.unnest(phyRelNode);

    /*
     * 1.3.b) Push down all the convert_fromjson expressions
     */
    phyRelNode = ComplexFunctionPushDownVisitor.convertFromJsonPushDown(phyRelNode, queryOptions);

    /*
     * 1.4) Break up all expressions with complex outputs into their own project operations
     *
     * This is not needed for planning anymore, but just in case there are udfs that needs to be split up, keep it.
     */
    phyRelNode =
        phyRelNode.accept(
            new SplitUpComplexExpressions.SplitUpComplexExpressionsVisitor(
                context.getFunctionRegistry()),
            null);

    /*
     * 2.)
     * Since our operators work via names rather than indices, we have to make to reorder any
     * output before we return data to the user as we may have accidentally shuffled things.
     * This adds a trivial project to reorder columns prior to output.
     */
    phyRelNode = FinalColumnReorderer.addFinalColumnOrdering(phyRelNode);

    /*
     * 2.5) Remove all exchanges in the following case:
     *   Leaf limits are disabled.
     *   Plan has no joins, window operators or aggregates (unions are okay)
     *   Plan has at least one subpattern that is scan > project > limit or scan > limit,
     *   The limit is 10k or less
     *   All scans are soft affinity
     */
    phyRelNode =
        SimpleLimitExchangeRemover.apply(config.getContext().getPlannerSettings(), phyRelNode);

    /*
     * 3.)
     * If two fragments are both estimated to be parallelization one, remove the exchange
     * separating them
     */
    /* DX-2353  should be fixed since it removes necessary exchanges and returns incorrect results. */
    long targetSliceSize = plannerSettings.getSliceTarget();
    phyRelNode = ExcessiveExchangeIdentifier.removeExcessiveEchanges(phyRelNode, targetSliceSize);

    /* 4.)
     * Add ProducerConsumer after each scan if the option is set
     * Use the configured queueSize
     */
    /* DRILL-1617 Disabling ProducerConsumer as it produces incorrect results
    if (context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER.getOptionName()).bool_val) {
      long queueSize = context.getOptions().getOption(PlannerSettings.PRODUCER_CONSUMER_QUEUE_SIZE.getOptionName()).num_val;
      phyRelNode = ProducerConsumerPrelVisitor.addProducerConsumerToScans(phyRelNode, (int) queueSize);
    }
    */

    /* 5.)
     * if the client does not support complex types (Map, Repeated)
     * insert a project which would convert
     */
    if (!context.getSession().isSupportComplexTypes()) {
      logger.debug("Client does not support complex types, add ComplexToJson operator.");
      phyRelNode = ComplexToJsonPrelVisitor.addComplexToJsonPrel(phyRelNode);
    }

    /* 5.5)
     * Insert additional required operations to achieve correct writer behavior
     */
    phyRelNode = WriterUpdater.update(phyRelNode);

    /* 5.5)
     * Insert Project before/after HashToMergeExchangePrel and HashToRandomExchangePrel nodes
     */
    phyRelNode = InsertHashProjectVisitor.insertHashProjects(phyRelNode, queryOptions);

    /* 6.)
     * Insert LocalExchange (mux and/or demux) nodes
     */
    phyRelNode =
        InsertLocalExchangeVisitor.insertLocalExchanges(
            phyRelNode, queryOptions, context.getGroupResourceInformation());

    /*
     * 7.)
     *
     * Convert any CONVERT_FROM(*, 'JSON') into a separate operator.
     */
    phyRelNode =
        phyRelNode.accept(new ConvertFromJsonConverter(context, phyRelNode.getCluster()), null);

    /*
     * 7.5.) Remove subtrees that are topped by a limit0.
     */
    phyRelNode = Limit0Converter.eliminateEmptyTrees(config, phyRelNode);

    // We need to keep this around, since redundant sort operations are introduced earlier in
    // physical planning
    phyRelNode = EmptyPrelPropagator.propagateEmptyPrel(config, phyRelNode);

    /* 8.)
     * Next, we add any required selection vector removers given the supported encodings of each
     * operator. This will ultimately move to a new trait but we're managing here for now to avoid
     * introducing new issues in planning before the next release
     */
    phyRelNode = SelectionVectorPrelVisitor.addSelectionRemoversWhereNecessary(phyRelNode);

    /* 9.)
     * Finally, Make sure that the no rels are repeats.
     * This could happen in the case of querying the same table twice as Optiq may canonicalize these.
     */
    phyRelNode = RelUniqifier.uniqifyGraph(phyRelNode);

    if (plannerSettings.applyCseBeforeRuntimeFilter()) {
      /*
       * Remove common sub expressions.
       */
      if (plannerSettings.isCSEEnabled()) {
        phyRelNode =
            CSEIdentifier.embellishAfterCommonSubExprElimination(config.getContext(), phyRelNode);
      }

      /*
       * add runtime filter information if applicable
       */
      if (plannerSettings.isRuntimeFilterEnabled()) {
        phyRelNode =
            RuntimeFilterDecorator.addRuntimeFilterToHashJoin(
                phyRelNode,
                plannerSettings
                    .getOptions()
                    .getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET));
      }
    } else {
      /*
       * 9.1)
       * add runtime filter information if applicable
       */
      if (plannerSettings.isRuntimeFilterEnabled()) {
        phyRelNode =
            RuntimeFilterDecorator.addRuntimeFilterToHashJoin(
                phyRelNode,
                plannerSettings
                    .getOptions()
                    .getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET));
      }

      /* 9.2)
       * Remove common sub expressions.
       */
      if (plannerSettings.isCSEEnabled()) {
        phyRelNode =
            CSEIdentifier.embellishAfterCommonSubExprElimination(config.getContext(), phyRelNode);
      }
    }

    phyRelNode = BridgeReaderSchemaFinder.findAndSetSchemas(phyRelNode, config.getContext());

    /* 10.0)
     * Expand nested functions. Need to do that here at the end of planning
     * so that we don't merge the projects back again.
     */
    phyRelNode = ExpandNestedFunctionVisitor.pushdownNestedFunctions(phyRelNode, queryOptions);

    /*
     * validate the join conditions after all prel transformation
     * */
    phyRelNode = JoinConditionValidatorVisitor.validate(phyRelNode, queryOptions);

    final String textPlan = PrelSequencer.getPlanText(phyRelNode, SqlExplainLevel.ALL_ATTRIBUTES);
    final String jsonPlan = PrelSequencer.getPlanJson(phyRelNode, SqlExplainLevel.ALL_ATTRIBUTES);
    if (logger.isDebugEnabled()) {
      logger.debug(String.format("%s:\n%s", "Final Physical Transformation", textPlan));
    }
    config.getObserver().planText(textPlan, finalPrelTimer.elapsed(TimeUnit.MILLISECONDS));
    config.getObserver().planJsonPlan(jsonPlan);

    config.getObserver().finalPrelPlanGenerated(phyRelNode);
    config.getObserver().setNumJoinsInFinalPrel(MoreRelOptUtil.countJoins(phyRelNode));
    return Pair.of(phyRelNode, textPlan);
  }

  public static PhysicalOperator convertToPop(SqlHandlerConfig config, Prel prel)
      throws IOException {
    PhysicalPlanCreator creator =
        new PhysicalPlanCreator(config.getContext(), PrelSequencer.getIdMap(prel));
    PhysicalOperator op = prel.getPhysicalOperator(creator);

    // Catch unresolvable "is_member()" function in plan and set the flag in query context
    // indicating that groups info needs to be available at executor.
    if (PrelUtil.containsCall(prel, "IS_MEMBER")) {
      config.getContext().setQueryRequiresGroupsInfo(true);
    }
    return op;
  }

  public static PhysicalPlan convertToPlan(
      SqlHandlerConfig config, PhysicalOperator op, Runnable committer, Runnable cleaner) {
    OptionList options = new OptionList();
    options.merge(config.getContext().getQueryOptionManager().getNonDefaultOptions());
    options.merge(config.getContext().getSessionOptionManager().getNonDefaultOptions());

    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.options(new JSONOptions(options));
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator("default", "handler");
    List<PhysicalOperator> ops = Lists.newArrayList();
    PopCollector c = new PopCollector();
    op.accept(c, ops);
    return new PhysicalPlan(propsBuilder.build(), ops, committer, cleaner);
  }

  public static PhysicalPlan convertToPlan(SqlHandlerConfig config, PhysicalOperator op) {
    return convertToPlan(config, op, null, null);
  }

  private static class PopCollector
      extends AbstractPhysicalVisitor<Void, Collection<PhysicalOperator>, RuntimeException> {

    @Override
    public Void visitOp(PhysicalOperator op, Collection<PhysicalOperator> collection)
        throws RuntimeException {
      collection.add(op);
      for (PhysicalOperator o : op) {
        o.accept(this, collection);
      }
      return null;
    }
  }
}
