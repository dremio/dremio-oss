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

import static com.dremio.exec.planner.physical.PlannerSettings.USE_LEGACY_DECORRELATOR;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.PlannerType;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.MaterializationList;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionInfo;
import com.dremio.exec.planner.common.ContainerRel;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.PlannerMetrics;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.DremioRelDecorrelator;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.EnhancedFilterJoinRule;
import com.dremio.exec.planner.logical.LegacyDremioRelDecorrelator;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.rule.LogicalAggregateGroupKeyFixRule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.dfs.FilesystemScanDrel;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import com.dremio.exec.work.foreman.UnsupportedRelOperatorException;
import com.dremio.sabot.op.join.JoinUtils;
import com.dremio.service.Pointer;
import com.google.common.collect.Lists;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DrelTransformer {
  public static final Logger LOGGER = LoggerFactory.getLogger(DrelTransformer.class);

  private DrelTransformer() {}

  /***
   * Converts to drel then adds a project to maintain the result names if necessary.
   *
   * @param config
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   * @throws RelConversionException
   */
  public static Rel convertToDrelMaintainingNames(SqlHandlerConfig config, RelNode relNode)
      throws SqlUnsupportedException {
    Rel drel = convertToDrel(config, relNode);
    return addRenamedProjectForMaterialization(config, drel, relNode.getRowType());
  }

  /**
   * Return Dremio Logical RelNode tree for a SELECT statement, when it is executed / explained
   * directly.
   *
   * @param relNode : root RelNode corresponds to Calcite Logical RelNode.
   * @param validatedRowType : the rowType for the final field names. A rename project may be placed
   *     on top of the root.
   * @return
   * @throws RelConversionException
   * @throws SqlUnsupportedException
   */
  public static Rel convertToDrel(
      SqlHandlerConfig config, RelNode relNode, RelDataType validatedRowType)
      throws RelConversionException, SqlUnsupportedException {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    Rel convertedRelNode = convertToDrel(config, relNode);

    // We might have to trim again after decorrelation ...
    Rel trimmedRelNode = (Rel) trimLogical(config, convertedRelNode);

    // Put a non-trivial topProject to ensure the final output field name is preserved, when
    // necessary.
    trimmedRelNode = addRenamedProject(config, trimmedRelNode, validatedRowType);

    trimmedRelNode =
        SqlHandlerUtil.storeQueryResultsIfNeeded(
            config.getConverter().getParserConfig(), config.getContext(), trimmedRelNode);
    return new ScreenRel(trimmedRelNode.getCluster(), trimmedRelNode.getTraitSet(), trimmedRelNode);
  }

  /**
   * Given a relNode tree for SELECT statement, convert to Dremio Logical RelNode tree.
   *
   * @param relNode
   * @return
   * @throws SqlUnsupportedException
   */
  public static Rel convertToDrel(SqlHandlerConfig config, final RelNode relNode)
      throws SqlUnsupportedException {
    try {
      final RelNode trimmed = trim(config, relNode);
      final RelNode flattenCaseExprs = flattenCaseExpression(config, trimmed);
      final RelNode rangeConditionRewrite = rewriteRangeConditions(config, flattenCaseExprs);

      final RelNode projPush = pushDownProjects(config, rangeConditionRewrite);
      final RelNode projPull = projectPullUp(config, projPush);
      final RelNode filterConstantPushdown = pushDownFilterConstant(config, projPull);
      final RelNode tfRel = pushDownTransitiveFilter(config, filterConstantPushdown);
      final RelNode conditionCanonicalization = equalityConditionCastCanonicalize(tfRel);
      final RelNode preLog = planPreLogical(config, conditionCanonicalization);
      final RelNode logical = planLogical(config, preLog);
      final RelNode sampledPlan = addSampling(config, logical);
      final RelNode rowCountAdjusted = adjustRowCount(config, sampledPlan);
      final RelNode trimmedGroupKeys = rewriteConstantGroupKey(rowCountAdjusted);
      final RelNode decorrelatedRel = decorrelate(config, trimmedGroupKeys);
      final RelNode jdbcPushDownRel = pushDownJdbcQuery(config, decorrelatedRel);
      final RelNode nestedProjectPushdown = nestedProjectPushdown(config, jdbcPushDownRel);
      final RelNode aggJoinPushed = pushDownAggregates(config, nestedProjectPushdown);
      final RelNode fixedGroupKeys = fixGroupKeys(aggJoinPushed);
      final RelNode trimmedAggJoinPushed = trimLogical(config, fixedGroupKeys);
      // Do Join Planning.

      final RelNode preConvertedRelNode = planMultiJoin(config, trimmedAggJoinPushed);
      final RelNode convertedRelNode = optimizeJoins(config, preConvertedRelNode);
      final RelNode postJoinOptimizationRelNode = postJoinOptimize(config, convertedRelNode);

      final RelNode flattendPushed = pushFlatten(config, postJoinOptimizationRelNode);
      final Rel drel = (Rel) flattendPushed;

      observeMaterialization(config, drel);
      return drel;
    } catch (RelOptPlanner.CannotPlanException ex) {
      LOGGER.error(ex.getMessage(), ex);

      if (JoinUtils.checkCartesianJoin(
          relNode, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList())) {
        throw new UnsupportedRelOperatorException(
            "This query cannot be planned\u2014possibly due to use of an unsupported feature.");
      } else {
        throw ex;
      }
    }
  }

  private static RelNode pushDownAggregates(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config,
        PlannerType.HEP_AC,
        PlannerPhase.AGG_JOIN_PUSHDOWN,
        relNode,
        relNode.getTraitSet(),
        true);
  }

  private static RelNode rewriteRangeConditions(SqlHandlerConfig config, RelNode relNode) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    return relNode.accept(new RangeConditionRewriteVisitor(plannerSettings));
  }

  private static RelNode postJoinOptimize(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config,
        PlannerType.HEP_AC,
        PlannerPhase.POST_JOIN_OPTIMIZATION,
        relNode,
        relNode.getTraitSet(),
        true);
  }

  private static RelNode optimizeJoins(SqlHandlerConfig config, RelNode relNode) {
    final RelNode preConvertedRelNode = planMultiJoin(config, relNode);
    return PlannerUtil.transform(
        config,
        PlannerType.HEP_BOTTOM_UP,
        PlannerPhase.JOIN_PLANNING_OPTIMIZATION,
        preConvertedRelNode,
        preConvertedRelNode.getTraitSet(),
        true);
  }

  private static RelNode planMultiJoin(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config,
        PlannerType.HEP_BOTTOM_UP,
        PlannerPhase.JOIN_PLANNING_MULTI_JOIN,
        relNode,
        relNode.getTraitSet(),
        true);
  }

  private static RelNode addSampling(SqlHandlerConfig config, RelNode relNode) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    return relNode.accept(new InjectSample(plannerSettings.isLeafLimitsEnabled()));
  }

  private static RelNode planLogical(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config,
        PlannerType.VOLCANO,
        PlannerPhase.LOGICAL,
        relNode,
        relNode.getTraitSet().plus(Rel.LOGICAL),
        true);
  }

  private static RelNode planPreLogical(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config, PlannerType.HEP_AC, PlannerPhase.PRE_LOGICAL, relNode, relNode.getTraitSet(), true);
  }

  private static RelNode pushDownFilterConstant(SqlHandlerConfig config, RelNode relNode) {
    RelNode filterPushdown =
        PlannerUtil.transform(
            config,
            PlannerType.HEP_AC,
            PlannerPhase.FILTER_CONSTANT_RESOLUTION_PUSHDOWN,
            relNode,
            relNode.getTraitSet(),
            true);

    return config.getContext().getPlannerSettings().useEnhancedFilterJoinGuardRail()
        ? EnhancedFilterJoinRule.removeArtifacts(filterPushdown)
        : filterPushdown;
  }

  private static RelNode pushDownProjects(SqlHandlerConfig config, RelNode relNode) {
    return PlannerUtil.transform(
        config,
        PlannerType.HEP_AC,
        PlannerPhase.PROJECT_PUSHDOWN,
        relNode,
        relNode.getTraitSet(),
        true);
  }

  private static RelNode equalityConditionCastCanonicalize(RelNode transitiveFilterPushdown) {
    return transitiveFilterPushdown.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            return this.visitChildren(
                other.accept(
                    new MoreRelOptUtil.EnsureEqualityTypeVisitor(
                        other.getCluster().getRexBuilder())));
          }
        });
  }

  private static RelNode trim(SqlHandlerConfig config, RelNode relNode) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    final RelBuilder relBuilder =
        DremioRelFactories.CALCITE_LOGICAL_BUILDER.create(relNode.getCluster(), null);
    DremioFieldTrimmer dremioFieldTrimmer =
        new DremioFieldTrimmer(
            relBuilder,
            DremioFieldTrimmerParameters.builder()
                .shouldLog(true)
                .isRelPlanning(true)
                .trimProjectedColumn(true)
                .trimJoinBranch(plannerSettings.trimJoinBranch())
                .build());
    return dremioFieldTrimmer.trim(relNode);
  }

  private static RelNode trimLogical(SqlHandlerConfig config, RelNode rel) {
    PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    RelBuilder relBuilder = DremioRelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
    // We might have to trim again after decorrelation ...
    DremioFieldTrimmer trimmer =
        new DremioFieldTrimmer(
            relBuilder,
            DremioFieldTrimmerParameters.builder()
                .shouldLog(true)
                .isRelPlanning(false)
                .trimProjectedColumn(true)
                .trimJoinBranch(plannerSettings.trimJoinBranch())
                .build());
    // Trimming twice, since some columns weren't being trimmed
    Rel trimmedRelNode = (Rel) trimmer.trim(trimmer.trim(rel));
    return trimmedRelNode;
  }

  private static RelNode flattenCaseExpression(SqlHandlerConfig config, RelNode relNode) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    return plannerSettings.getOptions().getOption(PlannerSettings.FLATTEN_CASE_EXPRS_ENABLED)
        ? FlattenCaseExpressionsVisitor.simplify(relNode)
        : relNode;
  }

  private static RelNode projectPullUp(SqlHandlerConfig config, RelNode projPush) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    if (plannerSettings.isProjectPullUpEnabled()) {
      return PlannerUtil.transform(
          config,
          PlannerType.HEP_AC,
          PlannerPhase.PROJECT_PULLUP,
          projPush,
          projPush.getTraitSet(),
          true);
    } else {
      return projPush;
    }
  }

  @WithSpan("PrelTransformer.transitiveFilterPushdown")
  private static RelNode pushDownTransitiveFilter(SqlHandlerConfig config, RelNode relNode) {
    PlannerSettings plannerSettings = config.getContext().getPlannerSettings();
    if (!plannerSettings.isTransitiveFilterPushdownEnabled()) {
      return relNode;
    }

    RelNode joinPullFilters = relNode.accept(new JoinPullTransitiveFiltersVisitor());
    RelNode filterPushedDown =
        PlannerUtil.transform(
            config,
            PlannerType.HEP_AC,
            PlannerPhase.FILTER_CONSTANT_RESOLUTION_PUSHDOWN,
            joinPullFilters,
            joinPullFilters.getTraitSet(),
            true);
    return config.getContext().getPlannerSettings().useEnhancedFilterJoinGuardRail()
        ? EnhancedFilterJoinRule.removeArtifacts(filterPushedDown)
        : filterPushedDown;
  }

  private static RelNode rewriteConstantGroupKey(RelNode relNode) {
    try {
      // Try removing multiple constants group keys from aggregates. Any unexpected failures in this
      // process shouldn't fail the whole query.
      return MoreRelOptUtil.removeConstantGroupKeys(relNode, DremioRelFactories.LOGICAL_BUILDER);
    } catch (Exception ex) {
      LOGGER.error("Failure while removing multiple constant group by keys in aggregate, ", ex);
      return relNode;
    }
  }

  private static RelNode decorrelate(SqlHandlerConfig config, RelNode relNode) {
    RelBuilder relBuilder = DremioRelFactories.LOGICAL_BUILDER.create(relNode.getCluster(), null);
    final RelNode decorrelatedQuery;
    if (config.getContext().getPlannerSettings().options.getOption(USE_LEGACY_DECORRELATOR)) {
      decorrelatedQuery = LegacyDremioRelDecorrelator.decorrelateQuery(relNode, relBuilder, true);
    } else {
      decorrelatedQuery = DremioRelDecorrelator.decorrelateQuery(relNode, relBuilder, true);
    }

    return decorrelatedQuery;
  }

  private static RelNode pushDownJdbcQuery(SqlHandlerConfig config, RelNode relNode) {
    RelBuilder relBuilder = DremioRelFactories.LOGICAL_BUILDER.create(relNode.getCluster(), null);
    PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    RelNode trimmedRel =
        new DremioFieldTrimmer(
                relBuilder,
                DremioFieldTrimmerParameters.builder()
                    .shouldLog(false)
                    .isRelPlanning(true)
                    .trimProjectedColumn(true)
                    .trimJoinBranch(plannerSettings.trimJoinBranch())
                    .build())
            .trim(relNode);

    Pointer<Boolean> hasJdbcPushDown = new Pointer<>(false);
    RelNode injectJdbcCrel =
        trimmedRel.accept(
            new RelShuttleImpl() {
              @Override
              public RelNode visit(TableScan scan) {
                if (scan instanceof JdbcRelImpl) {
                  hasJdbcPushDown.value = true;
                  return new JdbcCrel(
                      scan.getCluster(),
                      scan.getTraitSet().replace(Rel.LOGICAL),
                      scan,
                      ((JdbcRelImpl) scan).getPluginId());
                }
                return scan;
              }
            });

    if (!hasJdbcPushDown.value) {
      return relNode;
    }

    RelNode relationPlanningRel =
        PlannerUtil.transform(
            config,
            PlannerType.HEP_AC,
            PlannerPhase.RELATIONAL_PLANNING,
            injectJdbcCrel,
            injectJdbcCrel.getTraitSet().plus(Rel.LOGICAL),
            true);

    return relationPlanningRel
        .accept(new ShortenJdbcColumnAliases())
        .accept(new ConvertJdbcLogicalToJdbcRel(DremioRelFactories.LOGICAL_BUILDER));
  }

  private static RelNode nestedProjectPushdown(SqlHandlerConfig config, RelNode relNode) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    NestedFieldFinder nestedFieldFinder = new NestedFieldFinder();
    if (!plannerSettings.isNestedSchemaProjectPushdownEnabled()
        || !nestedFieldFinder.run(relNode)) {
      return relNode;
    }
    final RelNode wrapped = RexFieldAccessUtils.wrap(relNode, false);
    RelNode transformed =
        PlannerUtil.transform(
            config,
            PlannerType.HEP_AC,
            PlannerPhase.NESTED_SCHEMA_PROJECT_PUSHDOWN,
            wrapped,
            wrapped.getTraitSet(),
            true);
    RelNode unwrapped = RexFieldAccessUtils.unwrap(transformed);
    RelNode projectPushedDown =
        PlannerUtil.transform(
            config,
            PlannerType.HEP_BOTTOM_UP,
            PlannerPhase.FILESYSTEM_PROJECT_PUSHDOWN,
            unwrapped,
            unwrapped.getTraitSet(),
            true);

    return projectPushedDown;
  }

  private static RelNode pushFlatten(SqlHandlerConfig config, RelNode convertedRelNode) {
    FlattenRelFinder flattenFinder = new FlattenRelFinder();
    if (flattenFinder.run(convertedRelNode)) {
      final RelNode wrapped = RexFieldAccessUtils.wrap(convertedRelNode);
      RelNode transformed =
          PlannerUtil.transform(
              config,
              PlannerType.VOLCANO,
              PlannerPhase.FLATTEN_PUSHDOWN,
              wrapped,
              convertedRelNode.getTraitSet(),
              true);
      return RexFieldAccessUtils.unwrap(transformed);
    } else {
      return convertedRelNode;
    }
  }

  private static void observeMaterialization(SqlHandlerConfig config, Rel rel) {
    if (!(rel instanceof TableModify)) {
      final Optional<SubstitutionInfo> acceleration = findUsedMaterializations(config, rel);
      if (acceleration.isPresent()) {
        config.getObserver().planAccelerated(acceleration.get());
      } else {
        PlannerMetrics.ACCELERATED_QUERIES_COUNTER
            .withTag(PlannerMetrics.TAG_TARGET, "not_accelerated")
            .increment();
      }
    }
  }

  private static Rel addRenamedProject(
      SqlHandlerConfig config, Rel rel, RelDataType validatedRowType) {
    ProjectRel topProj = createRenameProjectRel(rel, validatedRowType);

    final boolean noneHaveAnyType =
        validatedRowType.getFieldList().stream()
            .noneMatch(input -> input.getType().getSqlTypeName() == SqlTypeName.ANY);

    // Add a final non-trivial Project to get the validatedRowType, if child is not project or the
    // input row type
    // contains at least one field of type ANY
    if (rel instanceof Project && MoreRelOptUtil.isTrivialProject(topProj) && noneHaveAnyType) {
      return rel;
    }

    return topProj;
  }

  public static Rel addRenamedProjectForMaterialization(
      SqlHandlerConfig config, Rel rel, RelDataType validatedRowType) {

    ProjectRel topProj = createRenameProjectRel(rel, validatedRowType);

    // Add a final non-trivial Project to get the validatedRowType
    if (MoreRelOptUtil.isTrivialProjectIgnoreNameCasing(topProj)) {
      return rel;
    }

    return topProj;
  }

  private static ProjectRel createRenameProjectRel(RelNode rel, RelDataType validatedRowType) {
    RelDataType t = rel.getRowType();

    RexBuilder b = rel.getCluster().getRexBuilder();
    List<RexNode> projections = Lists.newArrayList();
    int projectCount = validatedRowType.getFieldList().size();

    for (int i = 0; i < projectCount; i++) {
      projections.add(b.makeInputRef(rel, i));
    }

    final List<String> fieldNames2 =
        SqlValidatorUtil.uniquify(
            validatedRowType.getFieldNames(),
            SqlValidatorUtil.EXPR_SUGGESTER,
            rel.getCluster().getTypeFactory().getTypeSystem().isSchemaCaseSensitive());

    RelDataType newRowType =
        RexUtil.createStructType(rel.getCluster().getTypeFactory(), projections, fieldNames2);

    ProjectRel topProj =
        ProjectRel.create(rel.getCluster(), rel.getTraitSet(), rel, projections, newRowType);
    return topProj;
  }

  private static RelNode adjustRowCount(SqlHandlerConfig config, RelNode logical) {
    final PlannerSettings plannerSettings = config.getContext().getPlannerSettings();

    if (plannerSettings.removeRowCountAdjustment()) {
      return logical.accept(
          new RelShuttleImpl() {
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

  private static RelNode fixGroupKeys(RelNode relNode) {
    return relNode.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(RelNode rel) {
            rel = super.visit(rel);
            if (rel instanceof AggregateRel) {
              RelBuilder relBuilder =
                  DremioRelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
              return LogicalAggregateGroupKeyFixRule.fix((AggregateRel) rel, relBuilder);
            }
            return rel;
          }
        });
  }

  /**
   * Returns materializations used to accelerate this plan if any.
   *
   * <p>Returns an empty list if {@link MaterializationList materializations} is empty or plan is
   * not accelerated.
   *
   * @param root plan root to inspect
   */
  private static Optional<SubstitutionInfo> findUsedMaterializations(
      SqlHandlerConfig config, final RelNode root) {
    if (!config.getMaterializations().isPresent()) {
      return Optional.empty();
    }

    final SubstitutionInfo.Builder builder = SubstitutionInfo.builder();

    final MaterializationList table = config.getMaterializations().get();
    root.accept(
        new StatelessRelShuttleImpl() {
          @Override
          public RelNode visit(final TableScan scan) {
            final Optional<MaterializationDescriptor> descriptor =
                table.getDescriptor(scan.getTable().getQualifiedName());
            if (descriptor.isPresent()) {
              // Always use metadataQuery from the cluster (do not use calcite's default
              // CALCITE_INSTANCE)
              final RelOptCost cost = scan.getCluster().getMetadataQuery().getCumulativeCost(scan);
              final double acceleratedCost = DremioCost.aggregateCost(cost);
              final double originalCost = descriptor.get().getOriginalCost();
              final double speedUp = originalCost / acceleratedCost;
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
    final double acceleratedCost = DremioCost.aggregateCost(cost);
    builder.setCost(acceleratedCost);

    return Optional.of(info);
  }
}
