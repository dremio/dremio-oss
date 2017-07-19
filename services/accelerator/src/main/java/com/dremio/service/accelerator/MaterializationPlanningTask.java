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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.service.accelerator.DependencyGraph.DependencyNode;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.job.proto.Acceleration;
import com.dremio.service.job.proto.Acceleration.Substitution;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.TimePeriod;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

class MaterializationPlanningTask implements Runnable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializationPlanningTask.class);

  private final String acceleratorStorageName;
  private final JobsService jobsService;
  private final NamespaceService ns;
  private final Layout layout;
  private final Map<String, Layout> layouts;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final AtomicReference<Job> jobRef = new AtomicReference<>(null);
  private final AtomicReference<RelNode> logicalPlan = new AtomicReference<>(null);
  private final AtomicReference<Exception> exception = new AtomicReference<>(null);


  private final com.dremio.service.accelerator.proto.Acceleration acceleration;

  public List<DependencyNode> getDependencies() throws ExecutionSetupException {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
    if (logicalPlan.get() == null) {
      if (exception.get() != null) {
        throw new ExecutionSetupException("Logical Plan cannot be created", exception.get());
      } else {
        throw new ExecutionSetupException("Logical Plan cannot be created");
      }
    }
    Acceleration acceleration = jobRef.get().getJobAttempt().getInfo().getAcceleration();
    List<Substitution> substitutions = null;
    if (acceleration != null) {
      substitutions = acceleration.getSubstitutionsList();
    }
    substitutions = AccelerationUtils.selfOrEmpty(substitutions);
    List<DependencyNode> dependencies = FluentIterable.from(getScans(logicalPlan.get()))
        .transform(new Function<List<String>, DependencyNode>() {
          @Nullable
          @Override
          public DependencyNode apply(@Nullable List<String> datasetPath) {
            try {
              final DatasetConfig config = ns.getDataset(new NamespaceKey(datasetPath));
              final TimePeriod ttl = config.getPhysicalDataset().getAccelerationSettings().getAccelerationTTL();
              final long ttlInMillis = AccelerationUtils.toMillis(ttl);
              return new DependencyNode(datasetPath, ttlInMillis);
            } catch (NamespaceException e) {
              throw new RuntimeException(e);
            }
          }
        })
        .append(FluentIterable.from(substitutions)
            .transform(new Function<Substitution, Layout>() {
              @Nullable
              @Override
              public Layout apply(@Nullable Substitution sub) {
                return layouts.get(sub.getId().getLayoutId());
              }
            })
            .filter(new Predicate<Layout>() {
              @Override
              public boolean apply(@Nullable Layout layout) {
                return layout != null;
              }
            })
            .transform(new Function<Layout, DependencyNode>() {
              @Nullable
              @Override
              public DependencyNode apply(Layout layout) {
                return new DependencyNode(layout);
              }
            }))
        .toList();
    return dependencies;
  }

  private List<List<String>> getScans(RelNode logicalPlan) {
    final ImmutableList.Builder<List<String>> builder = ImmutableList.builder();
    logicalPlan.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(final TableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        if (!qualifiedName.get(0).equals(acceleratorStorageName)) {
          builder.add(qualifiedName);
        }
        return super.visit(scan);
      }
    });
    return builder.build();
  }

  public MaterializationPlanningTask(final String acceleratorStorageName,
      final JobsService jobsService,
      final Layout layout,
      final Map<String, Layout> layouts,
      final NamespaceService ns,
      final com.dremio.service.accelerator.proto.Acceleration acceleration) {
    this.acceleratorStorageName = acceleratorStorageName;
    this.jobsService = jobsService;
    this.layout = layout;
    this.layouts = layouts;
    this.ns = ns;
    this.acceleration = acceleration;
  }

  @Override
  public void run() {
    final String explainSql = getExplainSql();

    // cannot materialize from self
    final List<String> exclusions = ImmutableList.of(layout.getId().getId());
    final SqlQuery query = new SqlQuery(explainSql, SYSTEM_USERNAME);

    NamespaceKey datasetPathList = new NamespaceKey(acceleration.getContext().getDataset().getFullPathList());
    DatasetVersion datasetVersion = new DatasetVersion(acceleration.getContext().getDataset().getVersion());
    MaterializationSummary materializationSummary = new MaterializationSummary()
        .setAccelerationId(acceleration.getId().getId())
        .setLayoutVersion(layout.getVersion())
        .setLayoutId(layout.getId().getId());

    jobRef.set(jobsService.submitJobWithExclusions(query, QueryType.ACCELERATOR_EXPLAIN, datasetPathList, datasetVersion, exclusions, new JobStatusListener() {

      @Override
      public void jobSubmitted(JobId jobId) {

      }

      @Override
      public void planRelTansform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
        if (phase == PlannerPhase.LOGICAL) {
          logicalPlan.set(after);
        }
      }

      @Override
      public void metadataCollected(QueryMetadata metadata) {

      }

      @Override
      public void jobFailed(Exception e) {
        logger.warn("Failed to plan materialization", e);
        exception.set(e);
        latch.countDown();
      }

      @Override
      public void jobCompleted() {
        latch.countDown();
      }

      @Override
      public void jobCancelled() {
        latch.countDown();
      }
    }, materializationSummary));
  }

  String getExplainSql() {
    final List<String> source = ImmutableList.<String> builder()
        .add(AccelerationServiceImpl.MATERIALIZATION_STORAGE_PLUGIN_NAME)
        .add(String.format("%s", SqlUtils.quoteIdentifier(layout.getId().getId())))
        .build();

    final String viewSql = String.format("SELECT * FROM %s", Joiner.on(".").join(source));

    final String explainSql = String.format("EXPLAIN PLAN FOR %s", viewSql);
    return explainSql;
  }
}
