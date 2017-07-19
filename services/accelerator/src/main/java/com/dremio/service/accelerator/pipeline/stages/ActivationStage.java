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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.accelerator.AccelerationService;
import com.dremio.service.accelerator.AccelerationUtils;
import com.dremio.service.accelerator.AsyncTask;
import com.dremio.service.accelerator.DropTask;
import com.dremio.service.accelerator.MaterializationTask;
import com.dremio.service.accelerator.RebuildRefreshGraph;
import com.dremio.service.accelerator.pipeline.PipelineUtils;
import com.dremio.service.accelerator.pipeline.PipelineUtils.VersionedLayoutId;
import com.dremio.service.accelerator.pipeline.Stage;
import com.dremio.service.accelerator.pipeline.StageContext;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationMode;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainer;
import com.dremio.service.accelerator.proto.LayoutType;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.jobs.JobsService;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Activation stage
 */
public class ActivationStage implements Stage {
  private static final Logger logger = LoggerFactory.getLogger(ActivationStage.class);

  private final boolean rawEnabled;
  private final boolean aggregationEnabled;
  private final AccelerationState finalState;
  private final AccelerationMode finalMode;

  protected ActivationStage(final boolean rawEnabled, final boolean aggregationEnabled, final AccelerationState state,
      final AccelerationMode finalMode) {
    this.rawEnabled = rawEnabled;
    this.aggregationEnabled = aggregationEnabled;
    this.finalState = state;
    this.finalMode = finalMode;
  }

  @Override
  public void execute(final StageContext context) {
    logger.info("activating....");
    final Acceleration acceleration = context.getCurrentAcceleration();
    acceleration.setMode(finalMode);
    acceleration.getRawLayouts().setEnabled(rawEnabled);
    acceleration.getAggregationLayouts().setEnabled(aggregationEnabled);
    acceleration.setState(finalState);

    context.commit(acceleration);

    dropOldLayouts(context);
    materializeNewLayouts(context);
  }

  private void dropOldLayouts(final StageContext context) {
    final MaterializationStore materializationStore = context.getMaterializationStore();
    final JobsService jobsService = context.getJobsService();
    final ExecutorService executor = context.getExecutorService();

    final Acceleration original = context.getOriginalAcceleration();
    final Acceleration acceleration = context.getCurrentAcceleration();

    final Map<VersionedLayoutId, Layout> originalMappings = PipelineUtils.generateVersionedLayoutMapping(original);
    final Map<VersionedLayoutId, Layout> currentMappings = PipelineUtils.generateVersionedLayoutMapping(acceleration);

    // manually deleted entries
    final Iterable<Layout> deleted = FluentIterable
        .from(Sets.difference(originalMappings.keySet(), currentMappings.keySet()))
        .transform(new Function<VersionedLayoutId, Layout>() {
          @Override
          public Layout apply(final VersionedLayoutId input) {
            return originalMappings.get(input);
          }
        });

    final LayoutContainer rawContainer = original.getRawLayouts();
    final LayoutContainer aggContainer = original.getAggregationLayouts();

    // issue drop table exactly once populating all in a set
    final Iterable<Layout> droppedLayouts = ImmutableSet.<Layout>builder()
        .addAll(deleted)
        // delete all original raw layouts if raw is disabled
        .addAll(rawEnabled ? ImmutableList.<Layout>of() : AccelerationUtils.selfOrEmpty(rawContainer.getLayoutList()))
        // delete all original aggregation layouts if aggregation is disabled
        .addAll(aggregationEnabled ? ImmutableList.<Layout>of() : AccelerationUtils.selfOrEmpty(aggContainer.getLayoutList()))
        .build();

    final Iterable<Materialization> droppedMaterializations = FluentIterable
        .from(droppedLayouts)
        .transformAndConcat(new Function<Layout, Iterable<? extends Materialization>>() {
          @Nullable
          @Override
          public Iterable<? extends Materialization> apply(@Nullable final Layout input) {
            return AccelerationUtils.getAllMaterializations(materializationStore.get(input.getId()));
          }
        })
        .filter(new Predicate<Materialization>() {
          @Override
          public boolean apply(@Nullable final Materialization input) {
            return input.getState() == MaterializationState.DONE;
          }
        });

    for (final Materialization materialization : droppedMaterializations) {
      executor.execute(new DropTask(acceleration, materialization, jobsService));
    }

    for (final Layout layout : droppedLayouts) {
      materializationStore.remove(layout.getId());
    }
 }


  private void materializeNewLayouts(final StageContext context) {
    final Acceleration acceleration = context.getCurrentAcceleration();
    final List<Layout> newLayouts = findNewLayouts(context, acceleration);
    if (newLayouts.isEmpty()) {
      return;
    }

    final MaterializationStore store = context.getMaterializationStore();
    final JobsService jobsService = context.getJobsService();
    final AccelerationService accelerationService = context.getAccelerationService();
    final ExecutorService executor = context.getExecutorService();

    AsyncTask next = RebuildRefreshGraph.of(accelerationService);

    // note that new layout has aggregation layouts first in this loop we create tasks in reverse order so as to
    // materialize raw layouts before aggregation layouts
    for (final Layout layout : newLayouts) {
      MaterializationTask task = MaterializationTask.create(context.getAcceleratorStorageName(), store, jobsService,
        context.getNamespaceService(), context.getCatalogService(), layout, executor, acceleration, context.getAcceleratorStoragePlugin());
      task.setNext(next);
      next = task;
    }

    executor.submit(next);
  }

  private List<Layout> findNewLayouts(final StageContext context, final Acceleration acceleration) {
    final MaterializationStore store = context.getMaterializationStore();

    return FluentIterable
        .from(AccelerationUtils.allActiveLayouts(acceleration))
        .filter(new Predicate<Layout>() {
          @Override
          public boolean apply(@Nullable Layout layout) {
            final Optional<MaterializedLayout> materializedLayout = store.get(layout.getId());
            if (!materializedLayout.isPresent()) {
              return true;
            }

            return !Iterables.tryFind(AccelerationUtils.selfOrEmpty(materializedLayout.get().getMaterializationList()), new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable final Materialization input) {
                return input.getState() == MaterializationState.DONE;
              }
            }).isPresent();
          }
        })
        .toSortedList(new Comparator<Layout>() {
          @Override
          public int compare(Layout o1, Layout o2) {
            if (o1.getLayoutType() == o2.getLayoutType()) {
              return 0;
            }
            return o1.getLayoutType() == LayoutType.RAW ? 1 : -1;
          }
        });
  }


  public static ActivationStage of() {
    return of(false, false, AccelerationState.DISABLED, AccelerationMode.AUTO);
  }

  public static ActivationStage of(final boolean rawEnabled, final boolean aggregationEnabled,
      final AccelerationState finalState, final AccelerationMode finalMode) {
    return new ActivationStage(rawEnabled, aggregationEnabled, finalState, finalMode);
  }
}
