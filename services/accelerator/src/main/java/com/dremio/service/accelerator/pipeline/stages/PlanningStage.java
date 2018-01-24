/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.accelerator.pipeline.stages;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanSerializer;
import com.dremio.exec.planner.acceleration.normalization.Normalizer;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.Views;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.LayoutExpander;
import com.dremio.service.accelerator.analysis.AccelerationAnalyzer;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

import io.protostuff.ByteString;

/**
 * Plans layouts
 */
public class PlanningStage implements Stage {

  /**
   * Removes an update column from a persisted column. This means it won't be written and won't be used in matching.
   *
   * If there is not initial update column, no changes will be made.
   */
  public static RelNode removeUpdateColumn(RelNode node){
    if(node.getTraitSet() == null){
      // for test purposes.
      return node;
    }
    RelDataTypeField field = node.getRowType().getField(IncrementalUpdateUtils.UPDATE_COLUMN, false, false);
    if(field == null ){
      return node;
    }
    final RexBuilder rexBuilder = node.getCluster().getRexBuilder();
    final FieldInfoBuilder rowTypeBuilder = new FieldInfoBuilder(node.getCluster().getTypeFactory());
    final List<RexNode> projects = FluentIterable.from(node.getRowType().getFieldList())
    .filter(new Predicate<RelDataTypeField>(){
      @Override
      public boolean apply(RelDataTypeField input) {
        return !IncrementalUpdateUtils.UPDATE_COLUMN.equals(input.getName());
      }})
    .transform(new Function<RelDataTypeField, RexNode>(){
      @Override
      public RexNode apply(RelDataTypeField input) {
        rowTypeBuilder.add(input);
        return rexBuilder.makeInputRef(input.getType(), input.getIndex());
      }}).toList();

    return new LogicalProject(node.getCluster(), node.getTraitSet(), node, projects, rowTypeBuilder.build());
  }

  public static List<ViewFieldType> removeUpdateColumn(final List<ViewFieldType> fields) {
    return FluentIterable.from(fields).filter(new Predicate<ViewFieldType>() {
      @Override
      public boolean apply(@Nullable ViewFieldType input) {
        return !IncrementalUpdateUtils.UPDATE_COLUMN.equals(input.getName());
      }
    }).toList();
  }

  @Override
  public void execute(final StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();
    final JobsService jobsService = context.getJobsService();
    final NamespaceService namespaceService = context.getNamespaceService();
    final OptionManager optionManager = context.getOptionManager();

    final boolean removeProject = optionManager.getOption(ExecConstants.ACCELERATION_RAW_REMOVE_PROJECT);
    final boolean enableMinMax = optionManager.getOption(ExecConstants.ACCELERATION_ENABLE_MIN_MAX);
    final boolean useAlternateAnalysis = optionManager.getOption(ExecConstants.ACCELERATION_USE_ALTERNATE_SUGGESTION_ANALYSIS);

    final DatasetConfig config = context.getNamespaceService().findDatasetByUUID(acceleration.getId().getId());
    final NamespaceKey path = new NamespaceKey(config.getFullPathList());
    final RelNode accPlan = context.getAccelerationAnalysisPlan();
    final RelNode datasetPlan = removeUpdateColumn(null != accPlan ? accPlan : createAnalyzer(jobsService, useAlternateAnalysis).getPlan(path));

    final LogicalPlanSerializer serializer = createSerializer(datasetPlan.getCluster());
    final LayoutExpander expander = createExpander(datasetPlan, enableMinMax);

    final Iterable<Layout> layouts = AccelerationUtils.getAllLayouts(acceleration);
    for (final Layout layout : layouts) {
      final RelNode plan = expander.expand(layout);
      // DX-6734
      final Normalizer normalizer = (layout.getLayoutType() == LayoutType.AGGREGATION || removeProject) ? getAggregationViewNormalizer(context) : getRawViewNormalizer(context);

      final RelNode normalizedPlan = normalizer.normalize(plan);
      final ByteString planBytes = ByteString.copyFrom(serializer.serialize(normalizedPlan));

      final boolean incremental = IncrementalUpdateUtils.getIncremental(normalizedPlan, namespaceService);
      final String refreshField = !incremental ? null : IncrementalUpdateUtils.findRefreshField(normalizedPlan, namespaceService);

      // we use normalized schema in storage plugin during expansion to report rowtype
      // NEVER report/leak normalized schema to user because it is meaningless crap
      // user must see a subset of dataset schema not normalized schema.
      final RelDataType normalizedSchema = normalizedPlan.getRowType();
      final RowType schema = new RowType()
          .setFieldList(Views.viewToFieldTypes(Views.relDataTypeToFieldType(normalizedSchema)));

      layout
          .setLogicalPlan(planBytes)
          .setLayoutSchema(schema)
          .setIncremental(incremental)
          .setRefreshField(refreshField);
    }

    context.commit(acceleration);
  }

  @VisibleForTesting
  AccelerationAnalyzer createAnalyzer(final JobsService jobsService, final boolean useAlternateAnalysis) {
    return new AccelerationAnalyzer(jobsService, useAlternateAnalysis);
  }

  @VisibleForTesting
  LogicalPlanSerializer createSerializer(final RelOptCluster cluster) {
    return KryoLogicalPlanSerializers.forSerialization(cluster);
  }

  @VisibleForTesting
  LayoutExpander createExpander(final RelNode view, boolean enableMinMax) {
    return new LayoutExpander(view, enableMinMax);
  }

  public static final String DREMIO_ACCELERATOR_PLANNING_AGGREGATION_NORMALIZER = "dremio.accelerator.planning.aggregation-normalizer.class";
  public static final String DREMIO_ACCELERATOR_PLANNING_RAW_VIEW_NORMALIZER = "dremio.accelerator.planning.raw-view-normalizer.class";

  /**
   * A normalizer class doing nothing
   */
  public static final class PassThruNormalizer implements Normalizer {

    @Override
    public RelNode normalize(RelNode query) {
      return query;
    }
  };

  @VisibleForTesting
  Normalizer getAggregationViewNormalizer(final StageContext context) {
    return context.getConfig().getInstance(DREMIO_ACCELERATOR_PLANNING_AGGREGATION_NORMALIZER, Normalizer.class, PassThruNormalizer.class);
  }

  @VisibleForTesting
  Normalizer getRawViewNormalizer(final StageContext context) {
    return context.getConfig().getInstance(DREMIO_ACCELERATOR_PLANNING_RAW_VIEW_NORMALIZER, Normalizer.class, PassThruNormalizer.class);
  }

  public static PlanningStage of() {
    return new PlanningStage();
  }

}
