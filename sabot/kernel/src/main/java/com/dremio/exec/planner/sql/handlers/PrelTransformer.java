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
import static com.dremio.exec.planner.sql.handlers.RelTransformer.NO_OP_TRANSFORMER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.DremioRelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;

import com.dremio.common.JSONOptions;
import com.dremio.common.exceptions.UserException;
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
import com.dremio.exec.planner.MatchCountListener;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.substitution.AccelerationAwareSubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.common.ContainerRel;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.InvalidViewRel;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.ValuesRewriteShuttle;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.explain.PrelSequencer;
import com.dremio.exec.planner.physical.visitor.CSEIdentifier;
import com.dremio.exec.planner.physical.visitor.ComplexToJsonPrelVisitor;
import com.dremio.exec.planner.physical.visitor.EmptyPrelPropagator;
import com.dremio.exec.planner.physical.visitor.ExcessiveExchangeIdentifier;
import com.dremio.exec.planner.physical.visitor.FinalColumnReorderer;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryVisitor;
import com.dremio.exec.planner.physical.visitor.InsertHashProjectVisitor;
import com.dremio.exec.planner.physical.visitor.InsertLocalExchangeVisitor;
import com.dremio.exec.planner.physical.visitor.JoinPrelRenameVisitor;
import com.dremio.exec.planner.physical.visitor.RelUniqifier;
import com.dremio.exec.planner.physical.visitor.RuntimeFilterDecorator;
import com.dremio.exec.planner.physical.visitor.SelectionVectorPrelVisitor;
import com.dremio.exec.planner.physical.visitor.SimpleLimitExchangeRemover;
import com.dremio.exec.planner.physical.visitor.SplitCountChecker;
import com.dremio.exec.planner.physical.visitor.SplitUpComplexExpressions;
import com.dremio.exec.planner.physical.visitor.StarColumnConverter;
import com.dremio.exec.planner.physical.visitor.SwapHashJoinVisitor;
import com.dremio.exec.planner.physical.visitor.UnionAllExpander;
import com.dremio.exec.planner.physical.visitor.WriterUpdater;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlConverter.RelRootPlus;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.sql.parser.UnsupportedOperatorsVisitor;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.fromjson.ConvertFromJsonConverter;
import com.dremio.sabot.op.fromjson.ConvertFromJsonPushDownVisitor;
import com.dremio.sabot.op.join.JoinUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Collection of Rel, Drel and Prel transformations used in various planning cycles.
 */
public class PrelTransformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PrelTransformer.class);
  @SuppressWarnings("Slf4jIllegalPassedClass") // intentionally using logger from another class
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
    final SqlValidatorAndToRelContext sqlValidatorAndToRelContext =
      SqlValidatorAndToRelContext.builder(config.getConverter())
        .build();
    final Pair<SqlNode, RelDataType> validatedTypedSqlNode =
      validateNode(config, sqlValidatorAndToRelContext, sqlNode);
    if (config.getObserver() != null) {
      config.getObserver().beginState(AttemptObserver.toEvent(UserBitShared.AttemptEvent.State.PLANNING));
    }

    final SqlNode validated = validatedTypedSqlNode.getKey();
    final RelNode rel = convertToRel(config, sqlValidatorAndToRelContext, validated, relTransformer);
    final RelNode preprocessedRel = preprocessNode(config.getContext().getOperatorTable(), rel);
    return new ConvertedRelNode(preprocessedRel, validatedTypedSqlNode.getValue());
  }

  private static Pair<SqlNode, RelDataType> validateNode(SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode sqlNode) throws ValidationException, ForemanSetupException {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    final SqlNode sqlNodeValidated;

    try {
      sqlNodeValidated = sqlValidatorAndToRelContext.validate(sqlNode);
    } catch (final Throwable ex) {
      throw new ValidationException("unable to validate sql node", ex);
    }
    final Pair<SqlNode, RelDataType> typedSqlNode = new Pair<>(sqlNodeValidated, sqlValidatorAndToRelContext.getOutputType(sqlNodeValidated));

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

  public static RelNode trimFields(final RelNode relNode, boolean shouldLog, boolean isRelPlanning, boolean trimProjectedColumn) {
    final Stopwatch w = Stopwatch.createStarted();
    final RelFieldTrimmer trimmer = DremioFieldTrimmer.of(relNode.getCluster(), isRelPlanning, trimProjectedColumn);
    final RelNode trimmed = trimmer.trim(relNode);
    if(shouldLog) {
      log(PlannerType.HEP, PlannerPhase.FIELD_TRIMMING, trimmed, logger, w);
    }
    return trimmed;
  }

  public static RelNode trimFields(final RelNode relNode, boolean shouldLog, boolean isRelPlanning) {
    return trimFields(relNode, shouldLog, isRelPlanning, true);
  }

  /**
   *  Given a relNode tree for SELECT statement, convert to Dremio Logical RelNode tree.
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   */
  public static Rel convertToDrel(SqlHandlerConfig config, final RelNode relNode) throws SqlUnsupportedException {

    try {
      final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
      final RelNode trimmed = trimFields(relNode, true, true);
      final RelNode rangeConditionRewrite = trimmed.accept(new RangeConditionRewriteVisitor(plannerSettings));
      final RelNode projPush = transform(config, PlannerType.HEP_AC, PlannerPhase.PROJECT_PUSHDOWN, rangeConditionRewrite, rangeConditionRewrite.getTraitSet(), true);
      final RelNode expandOperators = expandOperators(config,projPush,plannerSettings);
      final RelNode projPull = projectPullUp(config,expandOperators,plannerSettings);
      final RelNode preLog = transform(config, PlannerType.HEP_AC, PlannerPhase.PRE_LOGICAL, projPull, projPull.getTraitSet(), true);
      final RelNode preLogTransitive = getPreLogicalTransitive(config, preLog, plannerSettings);
      final RelNode logical = transform(config, PlannerType.VOLCANO, PlannerPhase.LOGICAL, preLogTransitive, preLogTransitive.getTraitSet().plus(Rel.LOGICAL), true);
      final RelNode rowCountAdjusted = getRowCountAdjusted(logical, plannerSettings);
      final RelNode postLogical = getPostLogical(config, rowCountAdjusted, plannerSettings);
      final RelNode nestedProjectPushdown = getNestedProjectPushdown(config, postLogical, plannerSettings);
      // Do Join Planning.
      final RelNode preConvertedRelNode = transform(config, PlannerType.HEP_BOTTOM_UP, PlannerPhase.JOIN_PLANNING_MULTI_JOIN, nestedProjectPushdown, postLogical.getTraitSet(), true);
      final RelNode convertedRelNode = transform(config, PlannerType.HEP_BOTTOM_UP, PlannerPhase.JOIN_PLANNING_OPTIMIZATION, preConvertedRelNode, preConvertedRelNode.getTraitSet(), true);
      final RelNode postJoinOptimizationRelNode = transform(config, PlannerType.HEP_AC, PlannerPhase.POST_JOIN_OPTIMIZATION, convertedRelNode, convertedRelNode.getTraitSet(), true);
      final RelNode flattendPushed = getFlattenedPushed(config, postJoinOptimizationRelNode);
      final Rel drel = (Rel) flattendPushed;

      if (!(drel instanceof TableModify)) {
        final Optional<SubstitutionInfo> acceleration = findUsedMaterializations(config, drel);
        if (acceleration.isPresent()) {
          config.getObserver().planAccelerated(acceleration.get());
        }
      }
      return drel;
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage(), ex);

      if (JoinUtils.checkCartesianJoin(relNode, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned\u2014possibly due to use of an unsupported feature.");
      } else {
        throw ex;
      }
    }
  }

  private static RelNode expandOperators(SqlHandlerConfig config, RelNode projPush, PlannerSettings plannerSettings){
    if(plannerSettings.isExpandOperatorsEnabled()){
      return transform(config, PlannerType.HEP_AC, PlannerPhase.EXPAND_OPERATORS, projPush, projPush.getTraitSet(), true);
    }else{
      return projPush;
    }
  }
  private static RelNode projectPullUp(SqlHandlerConfig config, RelNode expandOperators, PlannerSettings plannerSettings){
    if(plannerSettings.isProjectPullUpEnabled()){
      return transform(config, PlannerType.HEP_AC, PlannerPhase.PROJECT_PULLUP, expandOperators, expandOperators.getTraitSet(), true);
    }else{
      return expandOperators;
    }
  }

  private static RelNode getPreLogicalTransitive(SqlHandlerConfig config, RelNode preLog, PlannerSettings plannerSettings) {
    if (plannerSettings.isTransitiveFilterPushdownEnabled()) {
      Stopwatch watch = Stopwatch.createStarted();
      final RelNode joinPullFilters = preLog.accept(new JoinPullTransitiveFiltersVisitor());
      log(PlannerType.HEP, PlannerPhase.TRANSITIVE_PREDICATE_PULLUP, joinPullFilters, logger, watch);
      config.getObserver().planRelTransform(PlannerPhase.TRANSITIVE_PREDICATE_PULLUP, null, preLog, joinPullFilters, watch.elapsed(TimeUnit.MILLISECONDS));
      return transform(config, PlannerType.HEP_AC, PlannerPhase.PRE_LOGICAL_TRANSITIVE, joinPullFilters, joinPullFilters.getTraitSet(), true);
    } else {
      return preLog;
    }
  }

  private static RelNode getRowCountAdjusted(RelNode logical, PlannerSettings plannerSettings) {
    if (plannerSettings.removeRowCountAdjustment()) {
      return logical.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          if (scan instanceof FilesystemScanDrel) {
            FilesystemScanDrel scanDrel = (FilesystemScanDrel) scan;
            return scanDrel.removeRowCountAdjustment();
          }
          return super.visit(scan);
        }
      });
    } else {
      return logical;
    }
  }

  private static RelNode getPostLogical(SqlHandlerConfig config, RelNode rowCountAdjusted, PlannerSettings plannerSettings) {
    RelNode relWithoutMultipleConstantGroupKey;
    try {
      // Try removing multiple constants group keys from aggregates. Any unexpected failures in this process shouldn't fail the whole query.
      relWithoutMultipleConstantGroupKey = MoreRelOptUtil.removeConstantGroupKeys(rowCountAdjusted, DremioRelFactories.LOGICAL_BUILDER);
    } catch (Exception ex) {
      logger.error("Failure while removing multiple constant group by keys in aggregate, ", ex);
      relWithoutMultipleConstantGroupKey = rowCountAdjusted;
    }
    final RelNode decorrelatedNode = DremioRelDecorrelator.decorrelateAndValidateQuery(relWithoutMultipleConstantGroupKey, DremioRelFactories.LOGICAL_BUILDER.create(relWithoutMultipleConstantGroupKey.getCluster(), null), true);
    final RelNode sortRemoved = (plannerSettings.isSortInJoinRemoverEnabled())? DremioSortInJoinRemover.remove(decorrelatedNode): decorrelatedNode;
    final RelNode jdbcPushDown = transform(config, PlannerType.HEP_AC, PlannerPhase.RELATIONAL_PLANNING, sortRemoved, sortRemoved.getTraitSet().plus(Rel.LOGICAL), true);
    return jdbcPushDown.accept(new ShortenJdbcColumnAliases()).accept(new ConvertJdbcLogicalToJdbcRel(DremioRelFactories.LOGICAL_BUILDER));
  }

  public static RelNode getNestedProjectPushdown(SqlHandlerConfig config, RelNode relNode, PlannerSettings plannerSettings){
    NestedFieldFinder nestedFieldFinder = new NestedFieldFinder();
    if(!plannerSettings.isNestedSchemaProjectPushdownEnabled()
    || !nestedFieldFinder.run(relNode)
    ){
      return relNode;
    }
    final RelNode wrapped = RexFieldAccessUtils.wrap(relNode, false);
    RelNode transformed = transform(config, PlannerType.HEP_AC, PlannerPhase.NESTED_SCHEMA_PROJECT_PUSHDOWN,
    wrapped, wrapped.getTraitSet(), true);
    RelNode unwrapped = RexFieldAccessUtils.unwrap(transformed);
    RelNode projectPushedDown = transform(config, PlannerType.HEP_BOTTOM_UP, PlannerPhase.FILESYSTEM_PROJECT_PUSHDOWN,
      unwrapped, unwrapped.getTraitSet(), true);

    return projectPushedDown;
  }

  private static RelNode getFlattenedPushed(SqlHandlerConfig config, RelNode convertedRelNode) {
    FlattenRelFinder flattenFinder = new FlattenRelFinder();
    if (flattenFinder.run(convertedRelNode)) {
      final RelNode wrapped = RexFieldAccessUtils.wrap(convertedRelNode);
      RelNode transformed = transform(config, PlannerType.VOLCANO, PlannerPhase.FLATTEN_PUSHDOWN,
        wrapped, convertedRelNode.getTraitSet(), true);
      return RexFieldAccessUtils.unwrap(transformed);
    } else {
      return convertedRelNode;
    }
  }

  /***
   * Converts to drel then adds a project to maintain the result names if necessary.
   *
   * @param config
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   * @throws RelConversionException
   */
  public static Rel convertToDrelMaintainingNames(
    SqlHandlerConfig config,
    RelNode relNode
  ) throws SqlUnsupportedException, RelConversionException {
    Rel drel = convertToDrel(config, relNode);
    return addRenamedProjectForMaterialization(config, drel, relNode.getRowType());
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

    final DremioFieldTrimmer trimmer = DremioFieldTrimmer.of(DremioRelFactories.LOGICAL_BUILDER.create(convertedRelNode.getCluster(), null));
    Rel trimmedRelNode = (Rel) trimmer.trim(convertedRelNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
    trimmedRelNode = addRenamedProject(config, trimmedRelNode, validatedRowType);

    trimmedRelNode = SqlHandlerUtil.storeQueryResultsIfNeeded(config.getConverter().getParserConfig(),
        config.getContext(), trimmedRelNode);
    return new ScreenRel(trimmedRelNode.getCluster(), trimmedRelNode.getTraitSet(), trimmedRelNode);
  }

  /**
   * Returns materializations used to accelerate this plan if any.
   *
   * Returns an empty list if {@link MaterializationList materializations} is empty or plan is not accelerated.
   * @param root plan root to inspect
   */
  private static Optional<SubstitutionInfo> findUsedMaterializations(SqlHandlerConfig config, final RelNode root) {
    if (!config.getMaterializations().isPresent()) {
      return Optional.empty();
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
      return Optional.empty();
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
  public static RelNode transform(
    SqlHandlerConfig config,
    PlannerType plannerType,
    PlannerPhase phase,
    final RelNode input,
    RelTraitSet targetTraits,
    boolean log
    ) {
    final RuleSet rules = config.getRules(phase);
    final RelTraitSet toTraits = targetTraits.simplify();
    final RelOptPlanner planner;
    final Supplier<RelNode> toPlan;
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    CALCITE_LOGGER.trace("Starting Planning for phase {} with target traits {}.", phase, targetTraits);
    if (Iterables.isEmpty(rules)) {
      CALCITE_LOGGER.trace("Completed Phase: {}. No rules.", phase);
      return input;
    }

    if(plannerType.isHep()) {

      final HepProgramBuilder hepPgmBldr = new HepProgramBuilder();

      long relNodeCount = MoreRelOptUtil.countRelNodes(input);
      long rulesCount = Iterables.size(rules);
      int matchLimit = (int) plannerSettings.getOptions().getOption(PlannerSettings.HEP_PLANNER_MATCH_LIMIT);
      hepPgmBldr.addMatchLimit(matchLimit);

      MatchCountListener matchCountListener = new MatchCountListener(relNodeCount, rulesCount, matchLimit,
        plannerSettings.getOptions().getOption(PlannerSettings.VERBOSE_RULE_MATCH_LISTENER));

      hepPgmBldr.addMatchOrder(plannerType.getMatchOrder());
      if(plannerType.isCombineRules()) {
        hepPgmBldr.addRuleCollection(Lists.newArrayList(rules));
      } else {
        for (RelOptRule rule : rules) {
          hepPgmBldr.addRuleInstance(rule);
        }
      }

      SqlConverter converter = config.getConverter();
      final DremioHepPlanner hepPlanner = new DremioHepPlanner(hepPgmBldr.build(), plannerSettings, converter.getCostFactory(), phase, matchCountListener);
      hepPlanner.setExecutor(new ConstExecutor(converter.getFunctionImplementationRegistry(), converter.getFunctionContext(), converter.getSettings()));

      // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
      RelOptCluster cluster = input.getCluster();
      cluster.setMetadataQuery(config.getContext().getRelMetadataQuerySupplier());
      cluster.invalidateMetadataQuery();

      // Begin planning
      hepPlanner.setRoot(input);
      if (!input.getTraitSet().equals(targetTraits)) {
        hepPlanner.changeTraits(input, toTraits);
      }

      planner = hepPlanner;
      toPlan = () -> {
        RelNode relNode = hepPlanner.findBestExp();
        if (log) {
          logger.debug("Phase: {}", phase);
          logger.debug(matchCountListener.toString());
        }
        return relNode;
      };
    } else {
      // as weird as it seems, the cluster's only planner is the volcano planner.
      Preconditions.checkArgument(input.getCluster().getPlanner() instanceof DremioVolcanoPlanner,
          "Cluster is expected to be constructed using DremioVolcanoPlanner. Was actually of type %s.",
          input.getCluster().getPlanner().getClass().getName());
      final DremioVolcanoPlanner volcanoPlanner = (DremioVolcanoPlanner) input.getCluster().getPlanner();
      volcanoPlanner.setPlannerPhase(phase);
      volcanoPlanner.setNoneConventionHasInfiniteCost((phase != PlannerPhase.JDBC_PUSHDOWN) && (phase != PlannerPhase.RELATIONAL_PLANNING));
      final Program program = Programs.of(rules);

      // Modify RelMetaProvider for every RelNode in the SQL operator Rel tree.
      RelOptCluster cluster = input.getCluster();
      cluster.setMetadataQuery(config.getContext().getRelMetadataQuerySupplier());
      cluster.invalidateMetadataQuery();

      // Configure substitutions
      final AccelerationAwareSubstitutionProvider substitutions = config.getConverter().getSubstitutionProvider();
      substitutions.setObserver(config.getObserver());
      substitutions.setEnabled(phase.useMaterializations);
      substitutions.setCurrentPlan(input);
      substitutions.setPostSubstitutionTransformers(
        ImmutableList.of(
          getPostSubstitutionTransformer(config, PlannerPhase.POST_SUBSTITUTION),
          getPostSubstitutionTransformer(config, PlannerPhase.POST_SUBSTITUTION_TRANSITIVE)));

      planner = volcanoPlanner;
      toPlan = () -> {
        try {
          RelNode relNode = program.run(volcanoPlanner, input, toTraits, ImmutableList.of(), ImmutableList.of());
          if (log) {
            logger.debug("Phase: {}", phase);
            logger.debug(volcanoPlanner.getMatchCountListener().toString());
          }
          return relNode;
        } finally {
          substitutions.setEnabled(false);
        }
      };
    }

    return doTransform(config, plannerType, phase, planner, input, log, toPlan);
  }

  public static RelTransformer getPostSubstitutionTransformer(SqlHandlerConfig config, PlannerPhase phase) {
    return relNode -> {
      final HepProgramBuilder builder = HepProgram.builder();
      builder.addMatchOrder(HepMatchOrder.ARBITRARY);
      builder.addRuleCollection(Lists.newArrayList(config.getRules(phase)));

      final HepProgram p = builder.build();

      final HepPlanner pl = new HepPlanner(p, config.getContext().getPlannerSettings());
      pl.setRoot(relNode);
      return pl.findBestExp().accept(new ConvertJdbcLogicalToJdbcRel(DremioRelFactories.CALCITE_LOGICAL_BUILDER));
    };
  }

  private static RelNode doTransform(SqlHandlerConfig config, final PlannerType plannerType, final PlannerPhase phase, final RelOptPlanner planner, final RelNode input, boolean log, Supplier<RelNode> toPlan) {
    final Stopwatch watch = Stopwatch.createStarted();

    try {
      final RelNode intermediateNode = toPlan.get();
      final RelNode output;
      if (phase == PlannerPhase.LOGICAL) {
        output = processBoostedMaterializations(config, intermediateNode);
      } else {
        output = intermediateNode;
      }

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

  private static RelNode processBoostedMaterializations(SqlHandlerConfig config, RelNode relNode) {
    final Set<List<String>> qualifiedNames = config.getMaterializations().isPresent() ?
      config.getMaterializations().get().getApplicableMaterializations()
        .stream()
        .filter(m -> m.getLayoutInfo().isArrowCachingEnabled())
        .map(DremioMaterialization::getTableRel)
        .map(rel -> {
          BoostMaterializationVisitor visitor = new BoostMaterializationVisitor();
          rel.accept(visitor);
          return visitor.getQualifiedName();
        })
        .collect(Collectors.toSet()) :
      new HashSet<>();
    if (qualifiedNames.isEmpty()) {
      return relNode;
    } else {
      // Only update the scans if there is any acceleration which is boosted
      return relNode.accept(new StatelessRelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          if (scan instanceof FilesystemScanDrel) {
            FilesystemScanDrel scanDrel = (FilesystemScanDrel) scan;
            if (qualifiedNames.contains(scanDrel.getTable().getQualifiedName())) {
              return scanDrel.applyArrowCachingEnabled(true);
            }
          }
          return super.visit(scan);
        }
      });
    }
  }

  private static class BoostMaterializationVisitor extends StatelessRelShuttleImpl {
    private List<String> qualifiedName = new ArrayList<>();

    @Override
    public RelNode visit(TableScan scan) {
      qualifiedName = scan.getTable().getQualifiedName();
      return super.visit(scan);
    }

    public List<String> getQualifiedName() {
      return qualifiedName;
    }
  }

  public static Pair<Prel, String> convertToPrel(SqlHandlerConfig config, RelNode drel) throws RelConversionException, SqlUnsupportedException {
    Preconditions.checkArgument(drel.getConvention() == Rel.LOGICAL);

    final RelTraitSet traits = drel.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON);
    Prel phyRelNode;
    try {
      final Stopwatch watch = Stopwatch.createStarted();
      final RelNode prel1 = transform(config, PlannerType.VOLCANO, PlannerPhase.PHYSICAL, drel, traits, true);

      final RelNode prel2 = transform(config, PlannerType.HEP_AC, PlannerPhase.PHYSICAL_HEP, prel1, prel1.getTraitSet(), true);
      phyRelNode = (Prel) prel2.accept(new PrelFinalizer());
      // log externally as we need to finalize before traversing the tree.
      log(PlannerType.VOLCANO, PlannerPhase.PHYSICAL, phyRelNode, logger, watch);
    } catch (RelOptPlanner.CannotPlanException ex) {
      logger.error(ex.getMessage());

      if(JoinUtils.checkCartesianJoin(drel, new ArrayList<>(), new ArrayList<>(), Lists.<Boolean>newArrayList())) {
        throw new UnsupportedRelOperatorException("This query cannot be planned\u2014possibly due to use of an unsupported feature.");
      } else {
        throw ex;
      }
    }
    return applyPhysicalPrelTransformations(config, phyRelNode);
  }

  public static Pair<Prel, String> applyPhysicalPrelTransformations(SqlHandlerConfig config, Prel phyRelNode) throws RelConversionException {
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
      phyRelNode = SwapHashJoinVisitor.swapHashJoin(phyRelNode, plannerSettings.getHashJoinSwapMarginFactor());
    }

    /*
     * 1.3) Push down all the convert_fromjson expressions
     */
    phyRelNode = ConvertFromJsonPushDownVisitor.convertFromJsonPushDown(phyRelNode, queryOptions);

    /*
     * 1.4) Break up all expressions with complex outputs into their own project operations
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
    phyRelNode = ExcessiveExchangeIdentifier.removeExcessiveExchanges(
      phyRelNode,
      plannerSettings.getSliceTarget());

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
    phyRelNode = InsertLocalExchangeVisitor.insertLocalExchanges(phyRelNode, queryOptions, context.getGroupResourceInformation());

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

    /* 7.7)
     * If a node is replaced by an EmptyPrel, certain operators coming after that node will be
     * empty like project or joins. Propagate the EmptyPrel and prune them here.
     */
    phyRelNode = EmptyPrelPropagator.propagateEmptyPrel(config, phyRelNode);

    /*
     * 7.8.) Expand UnionAlls with multiple inputs
     */
    if (plannerSettings.isUnionAllDistributeEnabled()) {
      phyRelNode = UnionAllExpander.expandUnionAlls(
        phyRelNode,
        config,
        plannerSettings.getSliceTarget());
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

    if (plannerSettings.applyCseBeforeRuntimeFilter()) {
      /*
       * Remove common sub expressions.
       */
      if (plannerSettings.isCSEEnabled()) {
        phyRelNode = CSEIdentifier.embellishAfterCommonSubExprElimination(config.getContext(), phyRelNode);
      }

      /*
       * add runtime filter information if applicable
       */
      if (plannerSettings.isRuntimeFilterEnabled()) {
        phyRelNode = RuntimeFilterDecorator
          .addRuntimeFilterToHashJoin(
            phyRelNode,
            plannerSettings.getOptions().getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET));
      }
    } else {
      /*
       * 9.1)
       * add runtime filter information if applicable
       */
      if (plannerSettings.isRuntimeFilterEnabled()) {
        phyRelNode = RuntimeFilterDecorator
          .addRuntimeFilterToHashJoin(
            phyRelNode,
            plannerSettings.getOptions().getOption(ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET));
      }

      /* 9.2)
       * Remove common sub expressions.
       */
      if (plannerSettings.isCSEEnabled()) {
        phyRelNode = CSEIdentifier.embellishAfterCommonSubExprElimination(config.getContext(), phyRelNode);
      }
    }

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

    // Catch unresolvable "is_member()" function in plan and set the flag in query context
    // indicating that groups info needs to be available at executor.
    if (PrelUtil.containsCall(prel, "IS_MEMBER")) {
      config.getContext().setQueryRequiresGroupsInfo(true);
    }
    return op;
  }

  public static PhysicalPlan convertToPlan(SqlHandlerConfig config, PhysicalOperator op, Runnable committer, Runnable cleaner) {
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

  private static RelNode toConvertibleRelRoot(SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode validatedNode,
      boolean expand,
      RelTransformer relTransformer) {
    final Stopwatch stopwatch = Stopwatch.createStarted();
    config.getConverter().getSubstitutionProvider().setPostSubstitutionTransformers(
      ImmutableList.of(
        getPostSubstitutionTransformer(config, PlannerPhase.POST_SUBSTITUTION),
        getPostSubstitutionTransformer(config, PlannerPhase.POST_SUBSTITUTION_TRANSITIVE)));
    config.getConverter().getSubstitutionProvider().setObserver(config.getObserver());

    final RelRootPlus convertible = sqlValidatorAndToRelContext.toConvertibleRelRoot(validatedNode, expand, true);
    final boolean currentPlanCacheable = config.getConverter().getFunctionContext().getContextInformation().isPlanCacheable();
    config.getConverter().getFunctionContext().getContextInformation().setPlanCacheable(
      currentPlanCacheable && convertible.isPlanCacheable());
    config.getObserver().planConvertedToRel(convertible.rel, stopwatch.elapsed(TimeUnit.MILLISECONDS));

    if(config.getContext().getOptions().getOption(PlannerSettings.VDS_AUTO_FIX)) {
      // verify that we don't need to refresh any VDS nodes.
      InvalidViewRel.checkForInvalid(config.getContext().getCatalog(), config.getConverter(), convertible.rel);
    }

    final RelNode reduced = relTransformer.transform(transform(config, PlannerType.HEP, PlannerPhase.REDUCE_EXPRESSIONS, convertible.rel, convertible.rel.getTraitSet(), true));
    config.getObserver().planSerializable(reduced);
    return reduced;
  }

  private static RelNode convertToRelRoot(SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode node,
      RelTransformer relTransformer) throws RelConversionException {
    final boolean leafLimitEnabled = config.getContext().getPlannerSettings().isLeafLimitsEnabled();

    final RelNode convertible = toConvertibleRelRoot(config, sqlValidatorAndToRelContext, node, true, relTransformer);



    // Convert with "expanding" exists/in subqueries (if applicable)
    // if we are having a RexSubQuery, this expanded tree will
    // have LogicalCorrelate rel nodes.
    final DremioVolcanoPlanner volcanoPlanner = (DremioVolcanoPlanner) convertible.getCluster().getPlanner();
    final RelNode originalRoot = convertible.accept(new InjectSample(leafLimitEnabled));
    volcanoPlanner.setOriginalRoot(originalRoot);
    return ExpansionNode.removeFromTree(convertible.accept(new InjectSample(leafLimitEnabled)));
  }

  private static RelNode convertToRel(SqlHandlerConfig config,
      SqlValidatorAndToRelContext sqlValidatorAndToRelContext,
      SqlNode node,
      RelTransformer relTransformer) throws RelConversionException {
    RelNode rel;
    final Catalog catalog = config.getContext().getCatalog();
    try {
      rel = convertToRelRoot(config, sqlValidatorAndToRelContext, node, relTransformer);

    } catch (RelConversionException e) {
      if (catalog instanceof CachingCatalog) {
        config.getObserver().tablesCollected(catalog.getAllRequestedTables());
      }
      throw e;
    }
    log("INITIAL", rel, logger, null);
    if (catalog instanceof CachingCatalog) {
      config.getObserver().tablesCollected(catalog.getAllRequestedTables());
    }
    RelNode expandedOperatorRel =
      transform(config, PlannerType.HEP_AC, PlannerPhase.OPERATOR_EXPANSION, rel, rel.getTraitSet(), true);
    if (RexSubQueryUtils.containsSubQuery(expandedOperatorRel)) {
      throw UserException.planError()
        .message("Failed to Decorrelate")
        .buildSilently();
    }
    return expandedOperatorRel;
  }

  public static RelNode preprocessNode(OperatorTable operatorTable, RelNode rel) throws SqlUnsupportedException {
    /*
     * Traverse the tree to do the following pre-processing tasks:
     *
     * 1) Replace the convert_from, convert_to function to
     * actual implementations Eg: convert_from(EXPR, 'JSON') be converted to convert_fromjson(EXPR);
     * TODO: Ideally all function rewrites would move here instead of RexToExpr.
     *
     * 2) See where the tree contains unsupported functions; throw SqlUnsupportedException if there is any.
     *
     * 3) Rewrite LogicalValue's row type and replace any Decimal tuples with double
     * since we don't support decimal type during execution.
     * See com.dremio.exec.planner.logical.ValuesRel.writeLiteral where we write Decimal as Double.
     * See com.dremio.exec.vector.complex.fn.VectorOutput.innerRun where we throw exception for Decimal type.
     */

    PreProcessRel visitor = PreProcessRel.createVisitor(
      operatorTable,
      rel.getCluster().getRexBuilder());
    try {
      rel = rel.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }

    rel = rel.accept(new ValuesRewriteShuttle());

    return rel;
  }

  private static Rel addRenamedProject(SqlHandlerConfig config, Rel rel, RelDataType validatedRowType) {
    ProjectRel topProj = createRenameProjectRel(rel, validatedRowType);

    final boolean noneHaveAnyType = validatedRowType.getFieldList().stream()
      .noneMatch(input -> input.getType().getSqlTypeName() == SqlTypeName.ANY);

    // Add a final non-trivial Project to get the validatedRowType, if child is not project or the input row type
    // contains at least one field of type ANY
    if (rel instanceof Project && MoreRelOptUtil.isTrivialProject(topProj) && noneHaveAnyType) {
      return rel;
    }

    return topProj;
  }

  private static Rel addRenamedProjectForMaterialization(SqlHandlerConfig config, Rel rel, RelDataType validatedRowType) {
    RelDataType relRowType = rel.getRowType();

    ProjectRel topProj = createRenameProjectRel(rel, validatedRowType);

    // Add a final non-trivial Project to get the validatedRowType
    if (MoreRelOptUtil.isTrivialProjectIgnoreNameCasing(topProj)) {
      return rel;
    }

    return topProj;
  }

  private static ProjectRel createRenameProjectRel(Rel rel, RelDataType validatedRowType) {
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
    return topProj;
  }
}
