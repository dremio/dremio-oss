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

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.utils.SqlUtils;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.fragment.Wrapper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin2.UpdateStatus;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FileSystemWriter;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.exec.work.user.SubstitutionSettings;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.DataPartition;
import com.dremio.service.accelerator.proto.JobDetails;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutField;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializatonFailure;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.PartitionDistributionStrategy;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.job.proto.Acceleration.Substitution;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobException;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.metadata.QueryMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * An asynchronous task that caches a given layout and updates metadata.
 */
public abstract class MaterializationTask {

  private static final Logger logger = LoggerFactory.getLogger(MaterializationTask.class);

  /**
   * results of a materialization task
   */
  public final class MaterializationResult {
    private final Job job;
    private final RelNode logicalNode;
    private final Materialization materialization;

    private MaterializationResult(Job job, RelNode logicalNode, Materialization materialization) {
      this.job = job;
      this.logicalNode = logicalNode;
      this.materialization = materialization;
    }

    public Job getJob() {
      return job;
    }

    public RelNode getLogicalNode() {
      return logicalNode;
    }

    public Materialization getMaterialization() {
      return materialization;
    }
  }

  /**
   * Context object to simplify passing all needed arguments to the materialization tasks
   */
  public static class MaterializationContext {
    private final String acceleratorStorageName;
    private final MaterializationStore materializationStore;

    private final JobsService jobsService;
    private final NamespaceService namespaceService;
    private final CatalogService catalogService;
    private final ExecutorService executorService;
    private final AccelerationService accelerationService;
    private final FileSystemPlugin accelerationStoragePlugin;

    public NamespaceService getNamespaceService() {
      return namespaceService;
    }

    public MaterializationContext(String acceleratorStorageName,
                                  MaterializationStore materializationStore,
                                  JobsService jobsService,
                                  NamespaceService namespaceService,
                                  CatalogService catalogService,
                                  ExecutorService executorService,
                                  AccelerationService accelerationService,
                                  FileSystemPlugin accelerationStoragePlugin) {
      this.acceleratorStorageName = acceleratorStorageName;
      this.materializationStore = materializationStore;
      this.jobsService = jobsService;
      this.namespaceService = namespaceService;
      this.catalogService = catalogService;
      this.executorService = executorService;
      this.accelerationService = accelerationService;
      this.accelerationStoragePlugin = accelerationStoragePlugin;
    }

    public AccelerationService getAccelerationService() {
      return accelerationService;
    }

    public String getAcceleratorStorageName() {
      return acceleratorStorageName;
    }

    public ExecutorService getExecutorService() {
      return executorService;
    }

    public MaterializationStore getMaterializationStore() {
      return materializationStore;
    }

    public JobsService getJobsService() {
      return jobsService;
    }

    public FileSystemPlugin getAccelerationStoragePlugin() {
      return accelerationStoragePlugin;
    }
  }

  private static final int MAX_TRIES = 10;
  private static final int MAX_BACKOFF = 1000;

  private final Layout layout;
  private final Acceleration acceleration;
  private final MaterializationContext context;

  private Materialization materialization;

  private final AtomicReference<Job> jobRef = new AtomicReference<>(null);

  private AtomicReference<RelNode> logicalPlan = new AtomicReference<>();

  private final FileSystemWrapper dfs;
  private final long gracePeriod;

  private final Path accelerationStoreLocation;

  private final SettableFuture<MaterializationResult> future = SettableFuture.create();

  MaterializationTask(final MaterializationContext materializationContext, Layout layout, Acceleration acceleration, long gracePeriod) {
    this.context = materializationContext;
    this.dfs = materializationContext.accelerationStoragePlugin.getFS(ImpersonationUtil.getProcessUserName());
    this.accelerationStoreLocation = new Path(materializationContext.accelerationStoragePlugin.getConfig().getPath());
    this.layout = layout;
    this.acceleration = acceleration;
    this.gracePeriod = gracePeriod;
  }

  public static MaterializationTask create(final MaterializationContext materializationContext,
                                           final Layout layout, final Acceleration acceleration, long gracePeriod) {
    boolean incremental = Optional.fromNullable(layout.getIncremental()).or(false);
    if (incremental) {
      return new IncrementalMaterializationTask(materializationContext, layout, acceleration, gracePeriod);
    } else {
      return new BasicMaterializationTask(materializationContext, layout, acceleration, gracePeriod);
    }
  }

  protected MaterializationContext getContext() {
    return context;
  }

  protected Layout getLayout() {
    return layout;
  }

  protected Materialization getMaterialization() {
    return materialization;
  }

  protected Job getJob() {
    return jobRef.get();
  }

  protected MaterializationId newRandomId() {
    return new MaterializationId(UUID.randomUUID().toString());
  }

  public ListenableFuture<MaterializationResult> materialize() {
    materialization = getOrCreateMaterialization();

    // if this is the first time we are materializing the layout, create and save a MaterializedLayout
    if (!context.materializationStore.get(materialization.getLayoutId()).isPresent()) {
      final MaterializedLayout materializedLayout = new MaterializedLayout()
        .setLayoutId(materialization.getLayoutId())
        .setMaterializationList(ImmutableList.of(materialization));
      context.materializationStore.save(materializedLayout);
    }

    final MaterializationId id = materialization.getId();
    final List<String> source = ImmutableList.<String>builder()
        .add(AccelerationServiceImpl.MATERIALIZATION_STORAGE_PLUGIN_NAME)
        .add(layout.getId().getId())
        .build();

    final String ctasSql = getCtasSql(source, id, materialization);

    // avoid accelerating this CTAS with the materialization itself
    final List<String> exclusions = ImmutableList.of(layout.getId().getId());
    final SqlQuery query = new SqlQuery(ctasSql, SYSTEM_USERNAME);
    final MaterializationStateListener listener = new MaterializationStateListener();
    NamespaceKey datasetPathList = new NamespaceKey(context.namespaceService.findDatasetByUUID(acceleration.getId().getId()).getFullPathList());
    DatasetVersion datasetVersion = new DatasetVersion(acceleration.getContext().getDataset().getVersion());
    MaterializationSummary materializationSummary = new MaterializationSummary()
        .setAccelerationId(acceleration.getId().getId())
        .setLayoutId(layout.getId().getId())
        .setLayoutVersion(layout.getVersion())
        .setMaterializationId(id.getId());

    jobRef.set(context.jobsService.submitJobWithExclusions(query, QueryType.ACCELERATOR_CREATE, datasetPathList,
        datasetVersion, listener, materializationSummary, new SubstitutionSettings(exclusions, false)));

    return future;
  }

  String getCTAS(List<String> destination, String viewSql){
    List<LayoutField> partitions = getLayout().getDetails().getPartitionFieldList();
    List<LayoutField> buckets = getLayout().getDetails().getDistributionFieldList();
    List<LayoutField> sorts = getLayout().getDetails().getSortFieldList();

    final StringBuilder sb = new StringBuilder()
        .append("CREATE TABLE ")
        .append(SqlUtils.quotedCompound(destination))
        .append(' ');


    if (!CollectionUtils.isEmpty(partitions)) {
      sb.append(getLayout().getDetails().getPartitionDistributionStrategy() == PartitionDistributionStrategy.STRIPED ?
          "STRIPED " : "HASH "); // STRIPED if STRIPED; HASH if default or CONSOLIDATED
      sb.append("PARTITION BY (");
      boolean first = true;
      for(LayoutField field : partitions){
        if(first){
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(SqlUtils.quoteIdentifier(field.getName()));
      }
      sb.append(") ");
    }

    if(!CollectionUtils.isEmpty(buckets)){
      sb.append("DISTRIBUTE BY (");
      boolean first = true;
      for(LayoutField field : buckets){
        if(first){
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(SqlUtils.quoteIdentifier(field.getName()));
      }
      sb.append(") ");
    }

    if(!CollectionUtils.isEmpty(sorts)){
      sb.append("LOCALSORT BY (");
      boolean first = true;
      for(LayoutField field : sorts){
        if(first){
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(SqlUtils.quoteIdentifier(field.getName()));
      }
      sb.append(") ");
    }

    sb.append(" AS ");
    sb.append(viewSql);
    return sb.toString();
  }

  protected abstract Materialization getOrCreateMaterialization();

  public abstract void updateMetadataAndSave();

  protected abstract String getCtasSql(List<String> source, MaterializationId id, Materialization materialization);

  /**
   * Returns a list of partitions into which acceleration/CTAS files get written.
   */
  private List<DataPartition> getPartitions(final PlanningSet planningSet) {
    final ImmutableSet.Builder<DataPartition> builder = ImmutableSet.builder();
    // visit every single major fragment and check to see if there is a PDFSWriter
    // if so add address of every minor fragment as a data partition to builder.
    for (final Wrapper majorFragment : planningSet) {
      majorFragment.getNode().getRoot().accept(new AbstractPhysicalVisitor<Object, Object, RuntimeException>() {
        @Override
        public Object visitOp(final PhysicalOperator op, final Object value) throws RuntimeException {
          // override to prevent throwing exception, super class throws an exception
          return visitChildren(op, value);
        }

        @Override
        public Object visitWriter(final Writer writer, final Object value) throws RuntimeException {
          // TODO DX-5438: Remove PDFS specific code
          if (writer instanceof FileSystemWriter && ((FileSystemWriter)writer).isPdfs()) {
            final List<DataPartition> addresses = Lists.transform(majorFragment.getAssignedEndpoints(),
                new Function<CoordinationProtos.NodeEndpoint, DataPartition>() {
                  @Nullable
                  @Override
                  public DataPartition apply(@Nullable final CoordinationProtos.NodeEndpoint endpoint) {
                    return new DataPartition(endpoint.getAddress());
                  }
                });
            builder.addAll(addresses);
          }
          return super.visitWriter(writer, value);
        }
      }, null);
    }

    return ImmutableList.copyOf(builder.build());
  }

  /**
   * Save materialization using random backoff with retries.
   * <p>
   * Note that this method is forgiving in that it just reports to the failure and quits without
   * notifying the caller. This is by design as we have no mechanism to prevent into fail-retry-fail loops.
   *
   * @param materialization materialization to save
   */
  protected void save(final Materialization materialization) {
    final String jobIdStr = materialization.getJob().getJobId();
    if (jobIdStr == null) {
      logger.warn("Accelerator state updated before getting jobId for path {} -> {}. Ignoring", layout.getId().getId(),
          materialization.getId().getId());
      return;
    }

    final JobId jobId = new JobId(jobIdStr);
    int tries = 0;

    do {
      final Optional<MaterializedLayout> currentLayout = context.materializationStore.get(materialization.getLayoutId());
      if (!currentLayout.isPresent()) {
        logger.info("unable to find layout {}. cancelling materialization.", materialization.getLayoutId().getId());
        cancelJob(jobId);
      }

      final List<Materialization> materializations = Lists.newArrayList(AccelerationUtils.selfOrEmpty(currentLayout.get().getMaterializationList()));

      // delete if the same materialization exists
      Iterables.removeIf(materializations, new Predicate<Materialization>() {
        @Override
        public boolean apply(@Nullable final Materialization input) {
          return materialization.getId().equals(input.getId());
        }
      });

      // eventually we will have to sort materializations chronologically as prepending here does not guarantee ordering
      materializations.add(materialization);
      currentLayout.get().setMaterializationList(materializations);

      try {
        context.materializationStore.save(currentLayout.get());
        return;
      } catch (final ConcurrentModificationException ex) {
        randomBackoff(MAX_BACKOFF);
      }
    } while (tries++ < MAX_TRIES);

    // Failed to update after MAX_TRIES
    logger.error("Failed to save materialization with params: {}",
        MoreObjects.toStringHelper(this).add("layoutId", materialization.getLayoutId().getId()));
    cancelJob(jobId);
  }

  private void cancelJob(final JobId id) {
    try {
      context.jobsService.cancel(SYSTEM_USERNAME, id);
    } catch(JobException e) {
      logger.warn("Cancellation failed for job {}", id, e);
    }
  }

  private void randomBackoff(final int maxSleep) {
    try {
      Thread.sleep((long) (Math.random() * maxSleep));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * The new materialization is only as fresh as the most stale input materialization. This method finds which of the
   * input materializations has the earliest expiration. The new materialization's expiration must be equal to or sooner
   * than this.
   * @return the earliest expiration. if no accelerations, Long.MAX_VALUE is returned
   */
  private long getFirstExpiration() {
    if (jobRef.get().getJobAttempt().getInfo().getAcceleration() == null) {
      return Long.MAX_VALUE;
    }
    List<Substitution> substitutions = jobRef.get().getJobAttempt().getInfo().getAcceleration().getSubstitutionsList();
    if (substitutions == null || substitutions.isEmpty()) {
      return Long.MAX_VALUE;
    }
    return FluentIterable.from(substitutions)
      .transform(new Function<Substitution, Long>() {
        @Nullable
        @Override
        public Long apply(Substitution substitution) {
          List<String> path = substitution.getTablePathList();
          final String layoutId = path.get(1);
          assert layoutId.equals(substitution.getId().getLayoutId()) : String.format("Layouts not equal. Layout id1: %s, Layout id2: %s", layoutId, substitution.getId().getLayoutId());
          final String materializationId = path.get(2);
          List<Materialization> materializationList = context.materializationStore.get(new LayoutId(layoutId)).get().getMaterializationList();
          Materialization materialization = Iterables.find(materializationList, new Predicate<Materialization>() {
            @Override
            public boolean apply(@Nullable Materialization materialization) {
              return materialization.getId().getId().equals(materializationId);
            }
          }, null);
          if (materialization == null || materialization.getExpiration() == null) {
            return 0L;
          }
          return materialization.getExpiration();
        }
      })
      .toSortedList(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return o1.compareTo(o2);
        }
      })
      .get(0); // the list is guaranteed to be non-empty because of check at beginning of method.
  }

  /**
   * Find all the scan against physical datasets, and return the minimum ttl value
   * If no physical datasets
   * @return the minimum ttl value
   */
  private Optional<Long> getTtl() {
    final ImmutableList.Builder<List<String>> builder = ImmutableList.builder();
    logicalPlan.get().accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(final TableScan scan) {
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        if (!qualifiedName.get(0).equals(context.acceleratorStorageName)) {
          builder.add(qualifiedName);
        }
        return super.visit(scan);
      }
    });
    List<List<String>> paths = builder.build();
    if (paths.isEmpty()) {
      return Optional.absent();
    }

    final long ttl = FluentIterable.from(paths)
      .transform(new Function<List<String>, Long>() {
        @Override
        public Long apply(List<String> path) {
          try {
            DatasetConfig datasetConfig = context.namespaceService.getDataset(new NamespaceKey(path));
            return datasetConfig.getPhysicalDataset().getAccelerationSettings().getGracePeriod();
          } catch (Exception e) {
            logger.warn("Failure extracting ttl", e);
            throw new RuntimeException(e);
          }
        }
      })
      .toSortedList(new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
          return o1.compareTo(o2);
        }
      })
      .get(0);
    return Optional.of(ttl);
  }

  //TODO move this logic to ChainExecutor
  void createMetadata(final Materialization materialization) {
    final List<LayoutField> sortedFields = Optional.fromNullable(layout.getDetails().getSortFieldList()).or(ImmutableList.<LayoutField>of());

    final Function<DatasetConfig, DatasetConfig> datasetMutator;
    if (sortedFields.isEmpty()) {
      datasetMutator = null;
    } else {
      datasetMutator = new Function<DatasetConfig, DatasetConfig>() {
        @Override
        public DatasetConfig apply(DatasetConfig datasetConfig) {
          if (datasetConfig.getReadDefinition() == null) {
            logger.warn("Trying to set sortColumnList on a datasetConfig that doesn't contain a read definition");
          } else {
            final List<String> sortColumnsList = FluentIterable.from(sortedFields)
              .transform(new Function<LayoutField, String>() {
                @Override
                public String apply(LayoutField field) {
                  return field.getName();
                }
              }).toList();
            datasetConfig.getReadDefinition().setSortColumnsList(sortColumnsList);
          }
          return datasetConfig;
        }
      };
    }

    context.catalogService.createDataset(new NamespaceKey(materialization.getPathList()), datasetMutator);
  }

  //TODO move this logic to ChainExecutor
  void refreshMetadata(final Materialization materialization) {
    try {
      final UpdateStatus status = context.catalogService.refreshDataset(new NamespaceKey(materialization.getPathList()));
      switch (status) {
        case CHANGED:
          break; // expected
        case UNCHANGED:
          logger.warn("A metadata for materialization {} wasn't updated after materialization task, " +
            "likely due to another refresh occurring between creation and this operation",
            materialization.getId().getId());
          break;
        default:
          throw new IllegalStateException(String.format("Invalid update status %s when updating metadata for materialization %s",
            status, materialization.getId().getId()));
      }
    } catch (NamespaceException e) {
      throw new IllegalStateException("Couldn't refresh materialization's metadata", e);
    }
  }

  private class MaterializationStateListener implements JobStatusListener {

    private void updateState(final MaterializationState newState) {
      // For incremental updates, the same materialization is updated, rather than creating a new one. But we
      // don't want to reset the materialization state to RUNNING, because that would result in the materialization being
      // temporarily unusable
      if ((newState == MaterializationState.RUNNING || newState == MaterializationState.FAILED)
          && materialization.getState() == MaterializationState.DONE) {
        return;
      }
      materialization.setState(newState);
      Job job = jobRef.get();
      if (job == null) {
        return;
      }

      final JobDetails details = materialization.getJob()
          .setJobId(job.getJobId().getId());

      JobAttempt config = job.getJobAttempt();
      if (config == null) {
        return;
      }

      final JobInfo info = config.getInfo();
      if (info != null) {
        details
            .setJobStart(config.getInfo().getStartTime())
            .setJobEnd(config.getInfo().getFinishTime());
      }

      JobStats stats = config.getStats();
      if (stats != null) {
        details
            .setInputBytes(stats.getInputBytes())
            .setInputRecords(stats.getInputRecords())
            .setOutputBytes(stats.getOutputBytes())
            .setOutputRecords(stats.getOutputRecords());
      }
    }

    @Override
    public void jobSubmitted(JobId jobId) {
      updateState(MaterializationState.RUNNING);
      save(materialization);
    }

    @Override
    public void planRelTansform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
      if (phase == PlannerPhase.LOGICAL) {
        logicalPlan.set(after);
      }
    }

    @Override
    public void metadataCollected(final QueryMetadata metadata) {
      // for now point cost is sum of all dimensions.
      final Optional<RelOptCost> cost = metadata.getCost();
      if (cost.isPresent()) {
        final double aggCost = DremioCost.aggregateCost(cost.get());
        materialization.getMetrics().setOriginalCost(aggCost);
      }
      materialization.setPartitionList(ImmutableList.<DataPartition>of());
      final Optional<PlanningSet> planningSet = metadata.getPlanningSet();
      if (planningSet.isPresent()) {
        final List<DataPartition> partitions = getPartitions(planningSet.get());
        materialization.setPartitionList(partitions);
      }
    }

    @Override
    public void jobFailed(final Exception e) {
      try {
        logger.error("unable to materialize the query. info: {}",
          MoreObjects.toStringHelper(MaterializationTask.class)
            .add("job", materialization.getJob().getJobId())
            .add("materialization", materialization.getId().getId())
            .add("layout", materialization.getLayoutId().getId())
          , e
        );
        updateState(MaterializationState.FAILED);
        // set failure details
        materialization
          .setFailure(new MaterializatonFailure()
            .setMessage(e.getMessage())
            .setStackTrace(AccelerationUtils.getStackTrace(e))
          );
        // TODO DX-7816 - Reclaim the any space used by materialization if the job is failed or cancelled
        save(materialization);
      } finally {
        future.setException(e);
      }
    }

    @Override
    public void jobCompleted() {
      try {
        updateState(MaterializationState.DONE);
        logger.info("job completed. info: {}",
            MoreObjects.toStringHelper(MaterializationTask.class)
                .add("job", materialization.getJob().getJobId())
                .add("materialization", materialization.getId().getId())
                .add("layout", materialization.getLayoutId().getId())
        );
        long ttl = getTtl().or(gracePeriod);
        long firstExpiration = getFirstExpiration();
        long jobStart = jobRef.get().getJobAttempt().getInfo().getStartTime();

        // if ttl is not set, use the default grace period
        long expiration = Math.max(ttl, jobStart + ttl); // prevent overflowing if ttl is infinite
        expiration = Math.min(expiration, firstExpiration);
        materialization.setExpiration(expiration);
        updateFootprint();

        final MaterializationResult result = new MaterializationResult(
          Preconditions.checkNotNull(jobRef.get(), "job cannot be null"),
          Preconditions.checkNotNull(logicalPlan.get(), "logical plan cannot be null"),
          materialization
        );
        if (!future.set(result)) {
          logger.warn("couldn't set value for materialization future as it has already been set or cancelled");
        }
      } catch (Throwable ex) {
        future.setException(ex);
      }
    }

    private void updateFootprint() {
      try {
        // TODO : DX-7814 -> Use a Generic method to calculate the space of a materialized  table
        materialization.getMetrics().setFootprint(dfs.getContentSummary(
          new Path(accelerationStoreLocation,
            StringUtils.join(FluentIterable.from(
              materialization.getPathList()).skip(1), "/"))).getSpaceConsumed());
      } catch (Throwable e) {
        logger.warn("Error while obtaining Materialization footprint info", e);
      }
    }

    @Override
    public void jobCancelled() {
      try {
        logger.info("materialization cancelled acceleration. info: {}",
          MoreObjects.toStringHelper(MaterializationTask.class)
            .add("job", materialization.getJob().getJobId())
            .add("materialization", materialization.getId().getId())
            .add("layout", materialization.getLayoutId().getId())
        );
        updateState(MaterializationState.FAILED);
        // TODO DX-7816 - Reclaim the any space used by materialization if the job is failed or cancelled
        save(materialization);
      } finally {
        // user cancelled the materialization, we treat this as a failure
        future.setException(new CancellationException("Materialization Job was cancelled"));
      }
    }
  }
}
