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

import static com.dremio.exec.planner.sql.handlers.RelTransformer.NO_OP_TRANSFORMER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;

import com.dremio.common.JSONOptions;
import com.dremio.common.logical.PlanProperties;
import com.dremio.common.logical.PlanProperties.Generator.ResultMode;
import com.dremio.common.logical.PlanProperties.PlanPropertiesBuilder;
import com.dremio.common.logical.PlanProperties.PlanType;
import com.dremio.exec.catalog.CachingCatalog;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.DremioHepPlanner;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.AccelerationAwareSubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.common.ContainerRel;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.logical.InvalidViewRel;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.visitor.ComplexToJsonPrelVisitor;
import com.dremio.exec.planner.physical.visitor.ExcessiveExchangeIdentifier;
import com.dremio.exec.planner.physical.visitor.FinalColumnReorderer;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryVisitor;
import com.dremio.exec.planner.physical.visitor.InsertHashProjectVisitor;
import com.dremio.exec.planner.physical.visitor.InsertLocalExchangeVisitor;
import com.dremio.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import com.dremio.exec.planner.physical.visitor.RelUniqifier;
import com.dremio.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import com.dremio.exec.planner.physical.visitor.SimpleLimitExchangeRemover;
import com.dremio.exec.planner.physical.visitor.SplitCountChecker;
import com.dremio.exec.planner.physical.visitor.SplitUpComplexExpressions;
import com.dremio.exec.planner.physical.visitor.StarColumnConverter;
import com.dremio.exec.planner.physical.visitor.SwapHashJoinVisitor;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils.FindNonJdbcConventionRexSubQuery;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils.RelsWithRexSubQueryTransformer;
import com.dremio.exec.planner.sql.parser.UnsupportedOperatorsVisitor;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.fromjson.ConvertFromJsonConverter;
import com.dremio.sabot.op.join.JoinUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Collection of Rel, Drel and Prel transformations used in various planning cycles.
 */
public class PrelTransformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrelTransformer.class);
  private static final org.slf4j.Logger CALCITE_LOGGER = org.slf4j.LoggerFactory.getLogger(RelOptPlanner.class);

  protected static void log(final PlannerType plannerType, final PlannerPhase phase, final RelNode node, final Logger logger,
      Stopwatch watch) {
    if (logger.isDebugEnabled()) {
      log(plannerType.name() + ":" + phase.description, node, logger, watch);
    }
  }

  public static void log(final String description, final RelNode node, final Logger logger, Stopwatch watch) {
    if (logger.isDebugEnabled()) {
      final String plan = RelOptUtil.toString(node, SqlExplainLevel.ALL_ATTRIBUTES);
      final String time = watch == null ? "" : String.format(" (%dms)", watch.elapsed(TimeUnit.MILLISECONDS));
      logger.debug(String.format("%s%s:\n%s", description, time, plan));
    }
  }

  public static void log(final SqlHandlerConfig config, final String name, final PhysicalPlan plan, final Logger logger) throws JsonProcessingException {
    if (logger.isDebugEnabled()) {
      String planText = plan.unparse(config.getContext().getLpPersistence().getMapper().writer());
      logger.debug(name + " : \n" + planText);
    }
  }

  public static ConvertedRelNode validateAndConvert(SqlHandlerConfig config, SqlNode sqlNode) throws ForemanSetupException, RelConversionException, ValidationException {
    return validateAndConvert(config, sqlNode, NO_OP_TRANSFORMER);
  }

  public static ConvertedRelNode validateAndConvert(SqlHandlerConfig config, SqlNode sqlNode, RelTransformer relTransformer) throws ForemanSetupException, RelConversionException, ValidationException {
    final Pair<SqlNode, RelDataType> validatedTypedSqlNode = validateNode(config, sqlNode);
    final SqlNode validated = validatedTypedSqlNode.getKey();
    final RelNode rel = convertToRel(config, validated, relTransformer);
    final RelNode preprocessedRel = preprocessNode(config, rel);
    return new ConvertedRelNode(preprocessedRel, validatedTypedSqlNode.getValue());
  }

  private static Pair<SqlNode, RelDataType> validateNode(SqlHandlerConfig config, final SqlNode sqlNode) throws ValidationException, RelConversionException, ForemanSetupException {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final SqlNode sqlNodeValidated;
    try {
      sqlNodeValidated = config.getConverter().validate(sqlNode);
    } catch (final Throwable ex) {
      throw new ValidationException("unable to validate sql node", ex);
    }
    final Pair<SqlNode, RelDataType> typedSqlNode = new Pair<>(sqlNodeValidated, config.getConverter().getOutputType(sqlNodeValidated));

    // Check if the unsupported functionality is used
    UnsupportedOperatorsVisitor visitor = UnsupportedOperatorsVisitor.createVisitor(config.getContext());
    try {
      sqlNodeValidated.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      // If the exception due to the unsupported functionalities
      visitor.convertException();

      // If it is not, let this exception move forward to higher logic
      throw ex;
    }

    config.getObserver().planValidated(typedSqlNode.getValue(), typedSqlNode.getKey(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
    return typedSqlNode;
  }

  public static RelNode trimFields(final RelNode relNode, boolean shouldLog) {
    final Stopwatch w = Stopwatch.createStarted();
    final RelFieldTrimmer trimmer = DremioFieldTrimmer.of(relNode.getCluster());
    final RelNode trimmed = trimmer.trim(relNode);
    if(shouldLog) {
      log(PlannerType.HEP, PlannerPhase.FIELD_TRIMMING, trimmed, logger, w);
    }
    return trimmed;
  }

  /**
   *  Given a relNode tree for SELECT statement, convert to Dremio Logical RelNode tree.
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   * @throws RelConversionException
   */
  public static Rel convertToDrel(SqlHandlerConfig config, final RelNode relNode) throws SqlUnsupportedException, RelConversionException {

    try {
      final RelNode trimmed = trimFields(relNode, true);
      final RelNode preLog = transform(config, PlannerType.HEP_AC, PlannerPhase.PRE_LOGICAL, trimmed, trimmed.getTraitSet(), true);

      final RelTraitSet logicalTraits = preLog.getTraitSet().plus(Rel.LOGICAL);
      final RelNode intermediateNode = transform(config, PlannerType.VOLCANO, PlannerPhase.LOGICAL, preLog, logicalTraits, true);

      final Catalog catalog = config.getContext().getCatalog();
      if (catalog instanceof CachingCatalog) {
        config.getObserver().tablesCollected(catalog.getAllRequestedTables());
      }

      // Do Join Planning.
      final RelNode preConvertedRelNode = transform(config, PlannerType.HEP_BOTTOM_UP, PlannerPhase.JOIN_PLANNING_MULTI_JOIN, intermediateNode, intermediateNode.getTraitSet(), true);
      final RelNode convertedRelNode = transform(config, PlannerType.HEP_BOTTOM_UP, PlannerPhase.JOIN_PLANNING_OPTIMIZATION, preConvertedRelNode, preConvertedRelNode.getTraitSet(), true);

      FlattenRelFinder flattenFinder = new FlattenRelFinder();
      final RelNode flattendPushed;
      if (flattenFinder.run(convertedRelNode)) {
        flattendPushed = transform(config, PlannerType.VOLCANO, PlannerPhase.FLATTEN_PUSHDOWN, convertedRelNode, convertedRelNode.getTraitSet(), true);
      } else {
        flattendPushed = convertedRelNode;
      }

      final Rel drel = (Rel) flattendPushed;

      if (drel instanceof TableModify) {
        throw new UnsupportedOperationException("TableModify " + drel);
      } else {
        final Optional<SubstitutionInfo> acceleration = findUsedMaterializations(config, drel);
        if (acceleration.isPresent()) {
          config.getObserver().planAccelerated(acceleration.get());
        }
        return drel;
      }
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage(), ex);

      if(JoinUtils.checkCartesianJoin(relNode, Lists.<Integer>newArrayList(), Lists.<Integer>newArrayList(), Lists.<Boolean>newArrayList())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned possibly due to either a cartesian join or an inequality join");
      } else {
        throw ex;
      }
    }
  }

  /**
   * Return Dremio Logical RelNode tree for a SELECT statement, when it is executed / explained directly.
   *
   * @param relNode : root RelNode corresponds to Calcite Logical RelNode.
   * @param validatedRowType : the rowType for the final field names. A rename project may be placed on top of the root.
   * @return
   * @throws RelConversionException
   * @throws SqlUnsupportedException
   */
  public static Rel convertToDrel(SqlHandlerConfig config, RelNode relNode, RelDataType validatedRowType) throws RelConversionException, SqlUnsupportedException {

    Rel convertedRelNode = convertToDrel(config, relNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
    convertedRelNode = addRenamedProject(config, convertedRelNode, validatedRowType);

    convertedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
        config.getContext(), convertedRelNode);

    return new ScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
  }

  /**
   * Returns materializations used to accelerate this plan if any.
   *
   * Returns an empty list if {@link MaterializationList materializations} is empty or plan is not accelerated.
   * @param root plan root to inspect
   */
  private static Optional<SubstitutionInfo> findUsedMaterializations(SqlHandlerConfig config, final RelNode root) {
    if (!config.getMaterializations().isPresent()) {
      return Optional.absent();
    }

    final SubstitutionInfo.Builder builder = SubstitutionInfo.builder();

    final MaterializationList table = config.getMaterializations().get();
    root.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(final TableScan scan) {
        final Optional<MaterializationDescriptor> descriptor = table.getDescriptor(scan.getTable().getQualifiedName());
        if (descriptor.isPresent()) {
          // Always use metadataQuery from the cluster (do not use calcite's default CALCITE_INSTANCE)
          final RelOptCost cost = scan.getCluster().getMetadataQuery().getCumulativeCost(scan);
          final double acceleratedCost = DremioCost.aggregateCost(cost);
          final double originalCost = descriptor.get().getOriginalCost();
          final double speedUp = originalCost/acceleratedCost;
          builder.addSubstitution(new SubstitutionInfo.Substitution(descriptor.get(), speedUp));
        }
        return super.visit(scan);
      }

      @Override
      public RelNode visit(final RelNode other) {
        if (other instanceof ContainerRel) {
          ContainerRel containerRel = (ContainerRel) other;
          return containerRel.getSubTree().accept(this);
        }
        return super.visit(other);
      }
    });

    final SubstitutionInfo info = builder.build();
    if (info.getSubstitutions().isEmpty()) {
      return Optional.absent();
    }

    // Some sources does not support retrieving cumulative cost like JDBC
    // moving this computation past the check above ensures that we do not inquire about the cost
    // until an acceleration is found.
    final RelOptCost cost = root.getCluster().getMetadataQuery().getCumulativeCost(root);
    final double acceleratedCost  = DremioCost.aggregateCost(cost);
    builder.setCost(acceleratedCost);


    return Optional.of(info);
  }

  /**
   * Transform RelNode to a new RelNode, targeting the provided set of traits. Also will log the outcome if asked.
   *
   * @param plannerType
   *          The type of Planner to use.
   * @param phase
   *          The transformation phase we're running.
   * @param input
   *          The original RelNode
   * @param targetTraits
   *          The traits we are targeting for output.
   * @param log
   *          Whether to log the planning phase.
   * @return The transformed relnode.
   */
  public static RelNode transform(SqlHandlerConfig config,
                           PlannerType plannerType,
                           PlannerPhase phase,
                           final RelNode input,
                           RelTraitSet targetTraits,
                           boolean log) {
    final RuleSet rules = config.getRules(phase);
    final RelTraitSet toTraits = targetTraits.simplify();
    final RelOptPlanner planner;
    final Supplier<RelNode> toPlan;

    CALCITE_LOGGER.trace("Starting Planning for phase {} with target traits {}.", phase, targetTraits);
    if (Iterables.isEmpty(rules)) {
      CALCITE_LOGGER.trace("Completed Phase: {}. No rules.");
      return input;
    }

    if(plannerType.isHep()) {

      final HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

      hepPgmBldr.addMatchOrder(plannerType.getMatchOrder());
      if(plannerType.isCombineRules()) {
        hepPgmBldr.addRuleCollection(Lists.newArrayList(rules));
      } else {
        for(RelOptRule rule : rules) {
          hepPgmBldr.addRuleInstance(rule);
        }
      }

      SqlConverter converter = config.getConverter();
      final HepPlanner hepPlanner = new DremioHepPlanner(hepPgmBldr.build(), config.getContext().getPlannerSettings(), converter.getCostFactory(), phase);
      hepPlanner.setExecutor(new ConstExecutor(converter.getFunctionImplementationRegistry(), converter.getFunctionContext(), converter.getSettings()));

      final List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(DefaultRelMetadataProvider.INSTANCE);
      hepPlanner.registerMetadataProviders(list);
      final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(
          ChainedRelMetadataProvider.of(list), hepPlanner);

      // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
      RelOptCluster cluster = input.getCluster();
      cluster.setMetadataProvider(cachingMetaDataProvider);
      cluster.invalidateMetadataQuery();

      // Begin planning
      hepPlanner.setRoot(input);
      if (!input.getTraitSet().equals(targetTraits)) {
        hepPlanner.changeTraits(input, toTraits);
      }

      planner = hepPlanner;
      toPlan = () -> hepPlanner.findBestExp();

    } else {
      // as weird as it seems, the cluster's only planner is the volcano planner.
      Preconditions.checkArgument(input.getCluster().getPlanner() instanceof DremioVolcanoPlanner,
          "Cluster is expected to be constructed using DremioVolcanoPlanner. Was actually of type %s.",
          input.getCluster().getPlanner().getClass().getName());
      final DremioVolcanoPlanner volcanoPlanner = (DremioVolcanoPlanner) input.getCluster().getPlanner();
      volcanoPlanner.setPlannerPhase(phase);
      volcanoPlanner.setNoneConventionHaveInfiniteCost(phase != PlannerPhase.JDBC_PUSHDOWN);
      final Program program = Programs.of(rules);

      final List<RelMetadataProvider> list = Lists.newArrayList();
      list.add(DefaultRelMetadataProvider.INSTANCE);
      volcanoPlanner.registerMetadataProviders(list);

      final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(
          ChainedRelMetadataProvider.of(list), volcanoPlanner);

      // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
      RelOptCluster cluster = input.getCluster();
      cluster.setMetadataProvider(cachingMetaDataProvider);
      cluster.invalidateMetadataQuery();

      // Configure substitutions
      final AccelerationAwareSubstitutionProvider substitutions = config.getConverter().getSubstitutionProvider();
      substitutions.setObserver(config.getObserver());
      substitutions.setEnabled(phase.useMaterializations);
      substitutions.setPostSubstitutionTransformer(
        relNode -> {
          final HepProgramBuilder builder = HepProgram.builder();
          builder.addMatchOrder(HepMatchOrder.ARBITRARY);
          builder.addRuleCollection(Lists.newArrayList(config.getRules(PlannerPhase.POST_SUBSTITUTION)));

          final HepProgram p = builder.build();

          final HepPlanner pl = new HepPlanner(p);
          pl.setRoot(relNode);
          return pl.findBestExp().accept(new ConvertJdbcLogicalToJdbcRel());
        }
      );

      planner = volcanoPlanner;
      toPlan = () -> {
        try {
          return program.run(volcanoPlanner, input, toTraits, ImmutableList.of(), ImmutableList.of());
        } finally {
          substitutions.setEnabled(false);
        }
      };
    }

    return doTransform(config, plannerType, phase, planner, input, log, toPlan);
  }

  private static RelNode doTransform(SqlHandlerConfig config, final PlannerType plannerType, final PlannerPhase phase, final RelOptPlanner planner, final RelNode input, boolean log, Supplier<RelNode> toPlan) {
    final Stopwatch watch = Stopwatch.createStarted();

    try {
      final RelNode output = toPlan.get();

      if (log) {
        log(plannerType, phase, output, logger, watch);
        config.getObserver().planRelTransform(phase, planner, input, output, watch.elapsed(TimeUnit.MILLISECONDS));
      }

      CALCITE_LOGGER.trace("Completed Phase: {}.", phase);

      return output;
    } catch (Throwable t) {
      // log our input state as oput anyway so we can ensure that we have details.
      try {
        log(plannerType, phase, input, logger, watch);
        config.getObserver().planRelTransform(phase, planner, input, input, watch.elapsed(TimeUnit.MILLISECONDS));
      } catch (Throwable unexpected) {
        t.addSuppressed(unexpected);
      }
      throw t;
    }
  }

  public static Pair<Prel, String> convertToPrel(SqlHandlerConfig config, RelNode drel) throws RelConversionException, SqlUnsupportedException {
    Preconditions.checkArgument(drel.getConvention() == Rel.LOGICAL);

    final RelTraitSet traits = drel.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON);
    Prel phyRelNode;
    try {
      final Stopwatch watch = Stopwatch.createStarted();
      // System.setProperty("calcite.debug", "true");
      final RelNode relNode = transform(config, PlannerType.VOLCANO, PlannerPhase.PHYSICAL, drel, traits, true);
      // System.clearProperty("calcite.debug");
      // PrintWriter pw = new PrintWriter(System.out);
      //((VolcanoPlanner)drel.getCluster().getPlanner()).dump(pw);
      // pw.flush();

      phyRelNode = (Prel) relNode.accept(new PrelFinalizer());
      // log externally as we need to finalize before traversing the tree.
      log(PlannerType.VOLCANO, PlannerPhase.PHYSICAL, phyRelNode, logger, watch);
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage());

      if(JoinUtils.checkCartesianJoin(drel, new ArrayList<Integer>(), new ArrayList<Integer>(), Lists.<Boolean>newArrayList())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned possibly due to either a cartesian join or an inequality join");
      } else {
        throw ex;
      }
    }
    QueryContext context = config.getContext();
    OptionManager queryOptions = context.getOptions();
    final PlannerSettings plannerSettings = context.getPlannerSettings();

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
    phyRelNode = SplitCountChecker.checkNumSplits(phyRelNode, plannerSettings.getQueryMaxSplitLimit(), plannerSettings.getDatasetMaxSplitLimit());

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
     * 1.)
     * Join might cause naming conflicts from its left and right child.
     * In such case, we have to insert Project to rename the conflicting names.
     */
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);

    /*
     * 1.2.) Swap left / right for INNER hash join, if left's row count is < (1 + margin) right's row count.
     * We want to have smaller dataset on the right side, since hash table builds on right side.
     */
    if (plannerSettings.isHashJoinSwapEnabled()) {
      phyRelNode = SwapHashJoinVisitor.swapHashJoin(phyRelNode, plannerSettings.getHashJoinSwapMarginFactor());
    }

    /*
     * 1.3.) Break up all expressions with complex outputs into their own project operations
     *
     * This is not needed for planning anymore, but just in case there are udfs that needs to be split up, keep it.
     */
    phyRelNode = phyRelNode.accept(
        new SplitUpComplexExpressions.SplitUpComplexExpressionsVisitor(
            context.getOperatorTable(),
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
    phyRelNode = SimpleLimitExchangeRemover.apply(config.getContext().getPlannerSettings(), phyRelNode);

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
     * insert a project which which would convert
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
    phyRelNode = InsertLocalExchangeVisitor.insertLocalExchanges(phyRelNode, queryOptions);

    /*
     * 7.)
     *
     * Convert any CONVERT_FROM(*, 'JSON') into a separate operator.
     */
    phyRelNode = phyRelNode.accept(new ConvertFromJsonConverter(context, phyRelNode.getCluster()), null);

    /*
     * 7.5.) Remove subtrees that are topped by a limit0.
     */
    phyRelNode = Limit0Converter.eliminateEmptyTrees(config, phyRelNode);

    /*
     * 7.6.)
     * Encode columns using dictionary encoding during scans and insert lookup before consuming dictionary ids.
     */
    if (plannerSettings.isGlobalDictionariesEnabled()) {
      phyRelNode = GlobalDictionaryVisitor.useGlobalDictionaries(phyRelNode);
    }

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

    final String textPlan;
    if (logger.isDebugEnabled() || config.getObserver() != null) {
      textPlan = PrelSequencer.setPlansWithIds(phyRelNode, SqlExplainLevel.ALL_ATTRIBUTES, config.getObserver(), finalPrelTimer.elapsed(TimeUnit.MILLISECONDS));
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("%s:\n%s", "Final Physical Transformation", textPlan));
      }
    } else {
      textPlan = "";
    }

    config.getObserver().finalPrel(phyRelNode);
    return Pair.of(phyRelNode, textPlan);
  }

  public static PhysicalOperator convertToPop(SqlHandlerConfig config, Prel prel) throws IOException {
    PhysicalPlanCreator creator = new PhysicalPlanCreator(config.getContext(), PrelSequencer.getIdMap(prel));
    PhysicalOperator op = prel.getPhysicalOperator(creator);
    return op;
  }

  public static PhysicalPlan convertToPlan(SqlHandlerConfig config, PhysicalOperator op) {
    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.options(new JSONOptions(config.getContext().getOptions().getOptionList()));
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator("default", "handler");
    List<PhysicalOperator> ops = Lists.newArrayList();
    PopCollector c = new PopCollector();
    op.accept(c, ops);
    return new PhysicalPlan(propsBuilder.build(), ops);
  }

  private static class PopCollector extends AbstractPhysicalVisitor<Void, Collection<PhysicalOperator>, RuntimeException> {

    @Override
    public Void visitOp(PhysicalOperator op, Collection<PhysicalOperator> collection) throws RuntimeException {
      collection.add(op);
      for (PhysicalOperator o : op) {
        o.accept(this, collection);
      }
      return null;
    }

  }

  private static RelNode toConvertibleRelRoot(SqlHandlerConfig config, final SqlNode validatedNode, boolean expand, RelTransformer relTransformer) {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final RelRootPlus convertible = config.getConverter().toConvertibleRelRoot(validatedNode, expand);
    config.getObserver().planConvertedToRel(convertible.rel, stopwatch.elapsed(TimeUnit.MILLISECONDS));

    if(config.getContext().getOptions().getOption(PlannerSettings.VDS_AUTO_FIX)) {
      // verify that we don't need to refresh any VDS nodes.
      InvalidViewRel.checkForInvalid(config.getContext().getCatalog(), config.getConverter(), convertible.rel);
    }

    final RelNode reduced = relTransformer.transform(transform(config, PlannerType.HEP, PlannerPhase.REDUCE_EXPRESSIONS, convertible.rel, convertible.rel.getTraitSet(), true));
    config.getObserver().planSerializable(reduced);
    return reduced;
  }

  private static RelNode convertToRelRootAndJdbc(SqlHandlerConfig config, SqlNode node, RelTransformer relTransformer) throws RelConversionException {

    // First try and convert without "expanding" exists/in/subqueries
    final RelNode convertible = toConvertibleRelRoot(config, node, false, relTransformer);

    // Check for RexSubQuery in the converted rel tree, and make sure that the table scans underlying
    // rel node with RexSubQuery have the same JDBC convention.
    final RelNode convertedNodeNotExpanded = convertible;
    RexSubQueryUtils.RexSubQueryPushdownChecker checker = new RexSubQueryUtils.RexSubQueryPushdownChecker(null);
    checker.visit(convertedNodeNotExpanded);

    final RelNode convertedNodeWithoutRexSubquery;
    final RelNode convertedNode;
    if (!checker.foundRexSubQuery()) {
      // If the not-expanded rel tree doesn't have any rex sub query, then everything is good.
      convertedNode = convertedNodeNotExpanded;
      convertedNodeWithoutRexSubquery = convertedNodeNotExpanded;
    } else {
      // If there is a rexSubQuery, then get the ones without (don't pass in SqlHandlerConfig here since we don't want to record it twice)
      convertedNodeWithoutRexSubquery = toConvertibleRelRoot(config, node, true, relTransformer);
      if (!checker.canPushdownRexSubQuery()) {
        // if there are RexSubQuery nodes with none-jdbc convention, abandon and expand the entire tree
        convertedNode = convertedNodeWithoutRexSubquery;
      } else {
        convertedNode = convertedNodeNotExpanded;
      }
    }

    final boolean leafLimitEnabled = config.getContext().getPlannerSettings().isLeafLimitsEnabled();

    { // Set original root in volcano planner for acceleration (in this case, do not inject JdbcCrel or JdbcRel)
      final DremioVolcanoPlanner volcanoPlanner = (DremioVolcanoPlanner) convertedNodeNotExpanded.getCluster().getPlanner();

      final RelNode originalRoot = convertedNodeWithoutRexSubquery.accept(new InjectSample(leafLimitEnabled));
      volcanoPlanner.setOriginalRoot(originalRoot);
    }

    // Now, transform jdbc nodes to Convention.NONE.  To do so, we need to inject a jdbc logical on top
    // of JDBC table scans with high cost and then plan to reduce the cost.
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final RelNode injectJdbcLogical = ExpansionNode.removeFromTree(convertedNode.accept(new InjectSample(leafLimitEnabled)));

    final RelNode jdbcPushedPartial = transform(config, PlannerType.HEP_AC, PlannerPhase.JDBC_PUSHDOWN, injectJdbcLogical, injectJdbcLogical.getTraitSet(), false);

    // Transform all the subquery reltree into jdbc as well! If any of them fail, we abort and just use the expanded reltree.
    final RelsWithRexSubQueryTransformer transformer = new RelsWithRexSubQueryTransformer(config);
    final RelNode jdbcPushed = jdbcPushedPartial.accept(transformer);

    // Check that we do not have non-jdbc subqueries, if we do, then we have to abort and do a complete conversion.
    final FindNonJdbcConventionRexSubQuery noRexSubQueryChecker = new FindNonJdbcConventionRexSubQuery();
    final boolean found = transformer.failed() ? false : noRexSubQueryChecker.visit(jdbcPushed);

    final RelNode finalConvertedNode;
    if (transformer.failed() || found) {
      log("Failed to pushdown RexSubquery. Applying JDBC pushdown to query with IN/EXISTS/SCALAR sub-queries converted to joins.", jdbcPushed, logger, null);
      final RelNode expandedWithSample = convertedNodeWithoutRexSubquery.accept(new InjectSample(leafLimitEnabled));
      finalConvertedNode = transform(config,PlannerType.HEP_AC, PlannerPhase.JDBC_PUSHDOWN, expandedWithSample,
        expandedWithSample.getTraitSet(), false).accept(new ConvertJdbcLogicalToJdbcRel());
    } else {
      finalConvertedNode = jdbcPushed.accept(new ShortenJdbcColumnAliases()).accept(new ConvertJdbcLogicalToJdbcRel());
    }
    config.getObserver().planRelTransform(PlannerPhase.JDBC_PUSHDOWN, null, convertedNode, finalConvertedNode, stopwatch.elapsed(TimeUnit.MILLISECONDS));



    return finalConvertedNode;
  }

  private static RelNode convertToRel(SqlHandlerConfig config, SqlNode node, RelTransformer relTransformer) throws RelConversionException {
    final RelNode rel = convertToRelRootAndJdbc(config, node, relTransformer);
    log("INITIAL", rel, logger, null);
    return transform(config, PlannerType.HEP, PlannerPhase.WINDOW_REWRITE, rel, rel.getTraitSet(), true);
  }

  private static RelNode preprocessNode(SqlHandlerConfig config, RelNode rel) throws SqlUnsupportedException {
    /*
     * Traverse the tree to do the following pre-processing tasks: 1. replace the convert_from, convert_to function to
     * actual implementations Eg: convert_from(EXPR, 'JSON') be converted to convert_fromjson(EXPR); TODO: Ideally all
     * function rewrites would move here instead of RexToExpr.
     *
     * 2. see where the tree contains unsupported functions; throw SqlUnsupportedException if there is any.
     */

    PreProcessRel visitor = PreProcessRel.createVisitor(
        config.getContext().getOperatorTable(),
        rel.getCluster().getRexBuilder());
    try {
      rel = rel.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }

    return rel;
  }

  public static Rel addRenamedProject(SqlHandlerConfig config, Rel rel, RelDataType validatedRowType) {
    RelDataType t = rel.getRowType();

    RexBuilder b = rel.getCluster().getRexBuilder();
    List<RexNode> projections = Lists.newArrayList();
    int projectCount = t.getFieldList().size();

    for (int i =0; i < projectCount; i++) {
      projections.add(b.makeInputRef(rel, i));
    }

    final List<String> fieldNames2 = SqlValidatorUtil.uniquify(
            validatedRowType.getFieldNames(),
            SqlValidatorUtil.F_SUGGESTER,
            rel.getCluster().getTypeFactory().getTypeSystem().isSchemaCaseSensitive());

    RelDataType newRowType = RexUtil.createStructType(rel.getCluster().getTypeFactory(), projections, fieldNames2);

    ProjectRel topProj = ProjectRel.create(rel.getCluster(), rel.getTraitSet(), rel, projections, newRowType);

    final boolean hasAnyType = Iterables.find(
        validatedRowType.getFieldList(),
        new Predicate<RelDataTypeField>() {
          @Override
          public boolean apply(@Nullable RelDataTypeField input) {
            return input.getType().getSqlTypeName() == SqlTypeName.ANY;
          }
        },
        null
    ) != null;

    // Add a final non-trivial Project to get the validatedRowType, if child is not project or the input row type
    // contains at least one field of type ANY
    if (rel instanceof Project && MoreRelOptUtil.isTrivialProject(topProj, true) && !hasAnyType) {
      return rel;
    }

    return topProj;
  }


}
