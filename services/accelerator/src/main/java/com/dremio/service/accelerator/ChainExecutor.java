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
package com.dremio.service.accelerator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.service.accelerator.MaterializationTask.MaterializationContext;
import com.dremio.service.accelerator.MaterializationTask.MaterializationResult;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.MaterializedLayoutState;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * runs a list of materialization tasks in order, and takes care of managing the overall state of the chain.
 */
public class ChainExecutor implements Runnable, AutoCloseable, Iterable<Layout> {
  private static final Logger logger = LoggerFactory.getLogger(ChainExecutor.class);
  private final DirectedGraph<DependencyNode, DefaultEdge> graph; // we use the graph to retrieve the dependencies for each layout
  private final List<Layout> layouts;
  private final Map<LayoutId, Integer> materializationFailures = new HashMap<>();
  private final long refreshPeriod;
  private final long gracePeriod;
  private final SchedulerService schedulerService;
  private final MaterializationContext context;
  private final boolean runOnce;
  private final boolean rebuildIfDependenciesChange;
  private final boolean rebuildGraphWhenDone;
  private final String id;

  private volatile long startTimeOfLastChain;

  /** true if the chain has been cancelled. Will continue refreshing all its layouts but won't reschedule once its done */
  private volatile boolean cancelled;
  private final int reflectionFailureThreshold;

  public ChainExecutor(List<Layout> layouts, MaterializationContext context,
      boolean rebuildGraphWhenDone, int reflectionFailureThreshold) {
    this.graph = null;
    this.layouts = Preconditions.checkNotNull(layouts, "layouts cannot be null");
    this.context = Preconditions.checkNotNull(context, "context cannot be null");
    startTimeOfLastChain = 0;
    refreshPeriod = 0;
    gracePeriod = Integer.MAX_VALUE; // use MAX_VALUE to have little effect on the ttl (we keep the min of all values)
    schedulerService = null;
    runOnce = true;
    rebuildIfDependenciesChange = false;
    this.rebuildGraphWhenDone = rebuildGraphWhenDone;
    this.reflectionFailureThreshold = reflectionFailureThreshold;
    this.id = null;
  }

  public ChainExecutor(DirectedGraph<DependencyNode, DefaultEdge> graph, List<Layout> layouts,
                       long startTimeOfLastChain, long refreshPeriod, long gracePeriod, SchedulerService schedulerService,
                       MaterializationContext context, int reflectionFailureThreshold, String id) {
    this.graph = Preconditions.checkNotNull(graph, "graph cannot be null");
    this.layouts = Preconditions.checkNotNull(layouts, "layouts cannot be null");
    this.startTimeOfLastChain = startTimeOfLastChain;
    this.refreshPeriod = refreshPeriod;
    this.gracePeriod = gracePeriod;
    this.schedulerService = Preconditions.checkNotNull(schedulerService, "scheduler service cannot be null");
    this.context = Preconditions.checkNotNull(context, "context cannot be null");
    runOnce = false;
    rebuildIfDependenciesChange = true;
    rebuildGraphWhenDone = false;
    this.reflectionFailureThreshold = reflectionFailureThreshold;
    this.id = id;
  }

  /**
   * Schedule the next run of materialization tasks.
   *
   * @return time in millis of the next run
   */
  long schedule() {
    if (cancelled) {
      return -1;
    }

    final long nextStartTime = startTimeOfLastChain + refreshPeriod;
    final long now = System.currentTimeMillis();
    if (nextStartTime <= now) {
      logger.debug("Scheduling sub graph {}, should have been {} but scheduling to run now.", id, nextStartTime);
      context.getExecutorService().submit(this);
      return now;
    } else {
      logger.debug("Scheduling sub graph {} for {}.", id, nextStartTime);
      schedulerService.schedule(ScheduleUtils.scheduleForRunningOnceAt(nextStartTime), this);
      return nextStartTime;
    }
  }

  @Override
  public void run() {
    if (cancelled) {
      return;
    }

    startTimeOfLastChain = System.currentTimeMillis();
    // if we detect a change in the dependencies we set this flag and rebuild at the end of the chain
    boolean rebuild = rebuildGraphWhenDone;

    logger.debug("Started running sub graph {}.", id);
    Stopwatch stopWatch = Stopwatch.createStarted();

    //run the layout
    for (final Layout layout : layouts) {
      try {
        Stopwatch layoutStopWatch = Stopwatch.createStarted();
        final Acceleration acceleration = getAcceleration(layout.getId());
        final MaterializationTask task = MaterializationTask.create(context, layout, acceleration, gracePeriod);

        final MaterializationResult result = task.materialize().get();

        // post materialization
        task.updateMetadataAndSave(startTimeOfLastChain);

        //update materialization cache
        context.getAccelerationService().getMaterlizationProvider().update(
          AccelerationUtils.getMaterializationDescriptor(acceleration, layout, result.getMaterialization(), true));

        if (!rebuild && rebuildIfDependenciesChange) {
          rebuild = shouldRebuild(acceleration, layout, result.getJob().getJobAttempt(), result.getLogicalNode());

          if (rebuild && logger.isDebugEnabled()) {
            logger.debug("While running sub graph {}, layout {} determined we need to rebuild the dependency graph.", id, layout.getId().getId());
          }
        }

        resetFailureCount(layout.getId());
        logger.debug("For sub graph {}, finished running layout {} successfully in {} ms.", id, layout.getId().getId(), layoutStopWatch.stop().elapsed(TimeUnit.MILLISECONDS));
      } catch (Throwable ex) {
        rebuild = materializationFailed(layout, ex) || rebuild;
        // continue
      }
    }

    logger.info("Finished running sub graph {} in {} ms.", id, stopWatch.stop().elapsed(TimeUnit.MILLISECONDS));

    if (cancelled) {
      return; // we should not attempt to rebuild the graph if we have been cancelled
    }
    if (rebuild) {
      context.getAccelerationService().startBuildDependencyGraph();
    } else if (!runOnce) {
      schedule();
    }
  }

  private void resetFailureCount(LayoutId id) {
    materializationFailures.remove(id);
  }
  /**
   * @return acceleration for given layoutId
   */
  private Acceleration getAcceleration(final LayoutId layoutId) {
    final Optional<Acceleration> acceleration = context.getAccelerationService().getAccelerationByLayoutId(layoutId);
    if (acceleration.isPresent()) {
      return acceleration.get();
    }

    throw new IllegalStateException(
      String.format("Trying to refresh layout %s for an acceleration that's no longer present", layoutId.getId())
    );
  }

  private boolean shouldRebuild(Acceleration acceleration, Layout layout, JobAttempt jobAttempt, RelNode logicalPlan) {

    final Set<DependencyNode> dependencies = Sets.newHashSet(Graphs.predecessorListOf(graph, new DependencyNode(layout)));
    final String accelerationStorageName = context.getAccelerationStoragePlugin().getName();
    List<DependencyNode> actualDependencies = MaterializationPlanningTask.constructDependencies(
      jobAttempt,
      logicalPlan,
      context.getNamespaceService(),
      context.getAccelerationService(),
      accelerationStorageName);

    //do not reconstruct the dependency graph if no dependencies are found and no records are found
    if (actualDependencies.isEmpty() && jobAttempt.getStats().getOutputRecords() == 0) {
      return false;
    }

    Set<DependencyNode> actualSet = Sets.newHashSet(actualDependencies);
    if (dependencies.equals(actualSet)) {
      return false;
    }

    // we need to run an explain plan to retrieve the dependencies
    MaterializationPlanningTask task = new MaterializationPlanningTask(
      accelerationStorageName,
      context.getJobsService(),
      layout,
      context.getNamespaceService(),
      context.getAccelerationService(),
      acceleration
    );

    task.run();
    Set<DependencyNode> updatedDependencies = null;
    try {
      updatedDependencies = Sets.newHashSet(task.getDependencies());
    } catch (ExecutionSetupException e) {
      Throwables.propagate(e);
    }

    return !dependencies.equals(updatedDependencies);
  }

  /**
   * returns whether the graph needs to be rebuilt or not
   * @param layout
   * @param ex
   * @return
   */
  private boolean materializationFailed(Layout layout, Throwable ex) {
    //ignore Cancellations
    if (ex instanceof CancellationException) {
      return false;
    }
    logger.warn("For sub graph {}, failed running layout {}.", id, layout.getId().getId(), ex);

    //obtain materializedLayout for the given layout
    int failureCount = materializationFailures.containsKey(layout.getId())?materializationFailures.get(layout.getId()):0;
    failureCount++;
    materializationFailures.put(layout.getId(), failureCount);

    //if failure count >= threshold, persist the count
    if (failureCount >= reflectionFailureThreshold) {
      logger.warn("{} consecutive failures while materializing layout with id = {}. Failure threshold is {}. Will not attempt again.",
          failureCount, layout.getId(), reflectionFailureThreshold);
      try {
        saveLayoutMaterializationFailedState(layout);
        return true;
      }catch(Throwable t) {
        logger.warn("Unable to update failure count for layout with id = " + layout.getId());
      }
    }
    return false;
  }

  private void saveLayoutMaterializationFailedState(Layout layout) {
    Optional<MaterializedLayout>  materializedLayoutOpt = context.getMaterializationStore().get(layout.getId());
    if (materializedLayoutOpt.isPresent()) {
      MaterializedLayout materializedLayout = materializedLayoutOpt.get();
      materializedLayout.setState(MaterializedLayoutState.FAILED);
      context.getMaterializationStore().save(materializedLayout);
    }
  }

  @Override
  public void close() {
    cancelled = true;
  }

  @Override
  public Iterator<Layout> iterator() {
    return Iterables.unmodifiableIterable(layouts).iterator();
  }
}
