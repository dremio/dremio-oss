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

import java.util.ConcurrentModificationException;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.acceleration.LogicalPlanSerializer;
import com.dremio.exec.store.Views;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.accelerator.analysis.AccelerationAnalysis;
import com.dremio.service.accelerator.analysis.AccelerationAnalyzer;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.pipeline.StageException;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationContext;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.protostuff.ByteString;

/**
 * Generates analysis
 */
public class AnalysisStage implements Stage {
  private static final Logger logger = LoggerFactory.getLogger(AnalysisStage.class);


  @Override
  public void execute(final StageContext context) {
    logger.info("analyzing....");
    try {
      tryExecuting(context);
    } catch (final ConcurrentModificationException ex) {
      Throwables.propagateIfPossible(ex);
    } catch (final Exception ex) {
      throw new StageException("Unable to recommend dimensions and measures. Please configure manually.", ex);
    }
  }

  private void tryExecuting(final StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();
    final DatasetConfig config = acceleration.getContext().getDataset();
    final NamespaceKey path = new NamespaceKey(config.getFullPathList());

    final AccelerationAnalyzer analyzer = new AccelerationAnalyzer(context.getJobsService());
    final AccelerationContext accelerationContext = acceleration.getContext();
    final AccelerationAnalysis analysis = analyzer.analyze(path);

    // 1. set plan
    final RelNode plan = PlanningStage.removeUpdateColumn(analysis.getPlan());
    final LogicalPlanSerializer serializer = KryoLogicalPlanSerializers.forSerialization(plan.getCluster());
    final ByteString planBytes = ByteString.copyFrom(serializer.serialize(plan));

    context.setAccelerationAnalysisPlan(plan);

    List<ViewFieldType> fields = ViewFieldsHelper.getViewFields(config);
    if (fields == null || fields.isEmpty()) {
      fields = Views.viewToFieldTypes(Views.relDataTypeToFieldType(plan.getRowType()));
    }
    fields = PlanningStage.removeUpdateColumn(fields);

    Preconditions.checkState(plan.getRowType().getFieldCount() == fields.size(),
      String.format("Found mismatching number of FieldTypes (%d) to RelDataTypes (%d):  %s, %s",
        fields.size(), plan.getRowType().getFieldCount(), fields, plan.getRowType().toString()));

    accelerationContext
        // save dataset plan
        .setDatasetPlan(planBytes)
        // dataset schema
        .setDatasetSchema(new RowType().setFieldList(fields));

    // 2. set analysis
    accelerationContext.setAnalysis(analysis.getDatasetAnalysis());

    context.commit(acceleration);
  }

  public static AnalysisStage of() {
    return new AnalysisStage();
  }

}
