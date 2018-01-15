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

import static com.dremio.service.accelerator.AccelerationUtils.selfOrEmpty;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.Instant;
import org.threeten.bp.temporal.TemporalAmount;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.concurrent.RenamingRunnable;
import com.dremio.datastore.IndexedStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.service.BindingCreator;
import com.dremio.service.accelerator.materialization.MaterializationStoragePluginConfig;
import com.dremio.service.accelerator.pipeline.PipelineManager;
import com.dremio.service.accelerator.proto.Acceleration;
import com.dremio.service.accelerator.proto.AccelerationDescriptor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.dremio.service.accelerator.proto.AccelerationId;
import com.dremio.service.accelerator.proto.AccelerationState;
import com.dremio.service.accelerator.proto.AccelerationStateDescriptor;
import com.dremio.service.accelerator.proto.DataPartition;
import com.dremio.service.accelerator.proto.Layout;
import com.dremio.service.accelerator.proto.LayoutContainerDescriptor;
import com.dremio.service.accelerator.proto.LayoutDescriptor;
import com.dremio.service.accelerator.proto.LayoutDetailsDescriptor;
import com.dremio.service.accelerator.proto.LayoutDimensionFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutFieldDescriptor;
import com.dremio.service.accelerator.proto.LayoutId;
import com.dremio.service.accelerator.proto.Materialization;
import com.dremio.service.accelerator.proto.MaterializationId;
import com.dremio.service.accelerator.proto.MaterializationState;
import com.dremio.service.accelerator.proto.MaterializedLayout;
import com.dremio.service.accelerator.proto.MaterializedLayoutState;
import com.dremio.service.accelerator.proto.RowType;
import com.dremio.service.accelerator.proto.SystemSettings;
import com.dremio.service.accelerator.store.AccelerationEntryStore;
import com.dremio.service.accelerator.store.AccelerationIndexKeys;
import com.dremio.service.accelerator.store.AccelerationStore;
import com.dremio.service.accelerator.store.MaterializationStore;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.ScheduleUtils;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * An implementation of {@link AccelerationService} with suggestions and scheduled materializations.
 */
public class AccelerationServiceImpl implements AccelerationService {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationServiceImpl.class);

  private static final Schedule EVERY_DAY = Schedule.Builder.everyDays(1).build();
  private static final Schedule EVERY_MINUTE = Schedule.Builder.everyMinutes(1).build();
  private static final TemporalAmount COMPACTION_GRACE_PERIOD = Duration.ofHours(4);
  public static final Ordering<Materialization> COMPLETION_ORDERING = Ordering.from(new Comparator<Materialization>() {
    @Override
    public int compare(final Materialization first, final Materialization second) {
      return Long.compare(first.getJob().getJobEnd(), second.getJob().getJobEnd());
    }
  });

  public static final Ordering<Materialization> STARTTIME_ORDERING = Ordering.from(new Comparator<Materialization>() {
    @Override
    public int compare(final Materialization first, final Materialization second) {
      // For materializations in DONE or RUNNING state, there will be a valid start date
      return Long.compare((first.getJob().getJobStart()), second.getJob().getJobStart());
    }
  });

  public static final String MATERIALIZATION_STORAGE_PLUGIN_NAME = "__materialization";

  private final Provider<SchedulerService> schedulerService;
  private final Provider<JobsService> jobsService;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<SabotContext> contextProvider;
  private final Provider<CatalogService> catalogService;
  private final BindingCreator bindingCreator;
  private final Provider<FileSystemPlugin> acceleratorStoragePluginProvider;

  private final Provider<NamespaceService> namespaceService;

  private volatile boolean queryAccelerationEnabled = true;

  private final ListeningExecutorService executor;
  private final DevService devService;

  private final PipelineManager pipelineManager;
  private final AccelerationStore accelerationStore;
  private final AccelerationEntryStore entryStore;
  private final MaterializationStore materializationStore;

  private CachedMaterializationProvider materializationProvider;
  private final AtomicReference<DependencyGraph> dependencyGraph = new AtomicReference<>();
  private MaterializationPlanningFactory materializationPlanningFactory = new MaterializationPlanningFactory();
  private AtomicReference<Cancellable> cleanupTaskRef = new AtomicReference<>();
  private Cancellable syncDatasetPathsTask;

  public AccelerationServiceImpl(
      final BindingCreator bindingCreator,
      final Provider<SabotContext> contextProvider,
      final Provider<KVStoreProvider> storeProvider,
      final Provider<SchedulerService> schedulerService,
      final Provider<JobsService> jobsService,
      final Provider<CatalogService> catalogService,
      final Provider<FileSystemPlugin> acceleratorStoragePluginProvider,
      final ExecutorService regularExecutor) {
    this.bindingCreator = bindingCreator;
    this.kvStoreProvider = Preconditions.checkNotNull(storeProvider, "store provider is required");
    this.schedulerService = Preconditions.checkNotNull(schedulerService, "scheduler service is required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service is required");
    this.executor = MoreExecutors.listeningDecorator(Preconditions.checkNotNull(regularExecutor, "executor is required"));
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service provider is required");
    this.contextProvider = contextProvider;
    this.acceleratorStoragePluginProvider = acceleratorStoragePluginProvider;
    this.devService = new DevService();
    this.accelerationStore = new AccelerationStore(kvStoreProvider);
    this.entryStore = new AccelerationEntryStore(kvStoreProvider);
    this.materializationStore = new MaterializationStore(kvStoreProvider);
    this.namespaceService = new Provider<NamespaceService>() {
      @Override
      public NamespaceService get() {
        return contextProvider.get().getNamespaceService(SYSTEM_USERNAME);
      }
    };

    final Provider<SabotConfig> sabotConfig = new Provider<SabotConfig>() {
      @Override
      public SabotConfig get() {
        return contextProvider.get().getConfig();
      }
    };
    final Provider<OptionManager> optionManager = new Provider<OptionManager>() {
      @Override
      public SystemOptionManager get() {
        return contextProvider.get().getOptionManager();
      }
    };
    this.pipelineManager = new PipelineManager(storeProvider, new Provider<String>() {
      @Override
      public String get() {
        return acceleratorStoragePluginProvider.get().getStorageName();
      }
    }, materializationStore, jobsService, namespaceService, catalogService, sabotConfig, optionManager, this, executor, acceleratorStoragePluginProvider);
  }

  @Override
  public void start() throws Exception {
    logger.info("Starting acceleration service");

    entryStore.start();
    accelerationStore.start();
    materializationStore.start();
    pipelineManager.start();
    startMaterializationPlugin();

    final MaterializationDescriptorProvider provider = new MaterializationDescriptorProvider() {
      @Override
      public List<MaterializationDescriptor> get() {
        return get(false);
      }

      @Override
      public void setDebug(boolean debug) {}

      @Override
      public Optional<MaterializationDescriptor> get(String materializationId) {
        List<MaterializationDescriptor> materializations = get();
        for (MaterializationDescriptor descriptor:materializations) {
          if (descriptor.getMaterializationId().equals(materializationId)) {
            return Optional.of(descriptor);
          }
        }
        return Optional.absent();
      }

      @Override
      public void update(MaterializationDescriptor materializationDescriptor) {
      }

      @Override
      public List<MaterializationDescriptor> get(boolean includeIncompleteDatasets) {
        if (!queryAccelerationEnabled) {
          return ImmutableList.of();
        }
        return getMaterializations(includeIncompleteDatasets);
      }

      @Override
      public void remove(String materializationId) {
      }

    };

    fixMaterializationStates();

    materializationProvider = new CachedMaterializationProvider(provider, contextProvider, this);
    materializationProvider.getStartedFuture().addListener(
      RenamingRunnable.of(RebuildRefreshGraph.of(this), "accel-refresher-startup"),
      executor
    );

    bindingCreator.replace(MaterializationDescriptorProvider.class, materializationProvider);

    bindingCreator.replace(AccelerationManager.class, new AccelerationManagerImpl(this, namespaceService.get()));
    schedulerService.get().schedule(EVERY_DAY, new EnableMostRequestedAccelerationTask(this));
    schedulerService.get().schedule(EVERY_DAY, new GarbageCollectorTask(this, COMPACTION_GRACE_PERIOD));
    cleanupTaskRef.set(scheduleCleanupTask());
    syncDatasetPathsTask = schedulerService.get().schedule(EVERY_MINUTE, new SyncDatasetPathsTask());
    logger.info("Acceleration service is up");
  }

  /**
   * Fix all materialization states so that materiaizations in {NEW, RUNNING} states are set to FAILED state.
   * Invoked once during startup
   */
  @VisibleForTesting
  public void fixMaterializationStates() {
    for (Layout layout: getActiveLayouts()) {
      Optional<MaterializedLayout> mlOptional = materializationStore.get(layout.getId());
      if (!mlOptional.isPresent()) {
        continue;
      }
      boolean bUpdated = false;
      MaterializedLayout MaterializedLayout = mlOptional.get();
      for (Materialization materialization: MaterializedLayout.getMaterializationList()) {
        if (materialization.getState() == MaterializationState.NEW ||
            materialization.getState() == MaterializationState.RUNNING) {
          bUpdated = true;
          materialization.setState(MaterializationState.FAILED);
        }
      }
      if (bUpdated) {
        materializationStore.save(MaterializedLayout);
      }
    }
  }

  @Override
  public MaterializationDescriptorProvider getMaterlizationProvider() {
    return materializationProvider;
  }

  private Cancellable scheduleCleanupTask() {
    return schedulerService.get().schedule(
        Schedule.Builder.everyMillis(getSettings().getOrphanCleanupInterval().longValue()).build(),
        new RemoveOrphanedAccelerations());
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(materializationProvider, pipelineManager);
    ScheduleUtils.cancel(syncDatasetPathsTask, cleanupTaskRef.get());
  }

  @Override
  public Iterable<Acceleration> getAccelerations(final IndexedStore.FindByCondition condition) {
    return accelerationStore.find(condition);
  }

  @Override
  public Iterable<Acceleration> getAllAccelerations() {
    return accelerationStore.find();
  }

  @Override
  public Optional<Acceleration> getAccelerationById(final AccelerationId id) {
    return accelerationStore.get(Preconditions.checkNotNull(id, "id is required"));
  }

  @Override
  public Optional<Acceleration> getAccelerationByLayoutId(LayoutId layoutId) {
    return accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, layoutId.getId());
  }

  @Override
  public Optional<AccelerationEntry> getAccelerationEntryById(final AccelerationId id) {
    return entryStore.get(id)
        .transform(new EntryRefresher());
  }

  @Override
  public Optional<AccelerationEntry> getAccelerationEntryByDataset(final NamespaceKey path) {
    final String pathString = Preconditions.checkNotNull(path, "path is required").getSchemaPath();
    try {
      final DatasetConfig datasetConfig = namespaceService.get().getDataset(path);
      return getAccelerationEntryById(new AccelerationId (datasetConfig.getId().getId()));
    } catch (NamespaceException e) {
      logger.warn("could not obtain dataset using path {}" , pathString, e);
      return Optional.absent();
    }
  }

  @Override
  public Optional<AccelerationEntry> getAccelerationEntryByLayoutId(final LayoutId layoutId) {
    return accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, layoutId.getId())
      .transform(new Function<Acceleration, AccelerationEntry>() {
        @Nullable
        @Override
        public AccelerationEntry apply(@Nullable final Acceleration input) {
          return entryStore.get(input.getId()).get();
        }
      })
      .transform(new EntryRefresher());
  }

  @Override
  public Optional<Layout> getLayout(final LayoutId layoutId) {
    final Optional<Acceleration> acceleration = accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, layoutId.getId());
    if (!acceleration.isPresent()) {
      return null;
    }

    return FluentIterable.from(AccelerationUtils.allLayouts(acceleration.get()))
      .firstMatch(new Predicate<Layout>() {
        @Override
        public boolean apply( Layout layout) {
          return layout.getId().equals(layoutId);
        }
      });
  }

  @Override
  public Optional<AccelerationEntry> getAccelerationEntryByJob(final JobId jobId) {
    final String jobIdString = Preconditions.checkNotNull(jobId, "jobId is required").getId();
    return accelerationStore.getByIndex(AccelerationIndexKeys.JOB_ID, jobIdString)
        .transform(new Function<Acceleration, AccelerationEntry>() {
          @Nullable
          @Override
          public AccelerationEntry apply(@Nullable final Acceleration input) {
            return entryStore.get(input.getId()).get();
          }
        })
        .transform(new EntryRefresher());
  }

  @Override
  public boolean isMaterializationCached(MaterializationId materializationId) {
    return materializationProvider.get(materializationId.getId()).isPresent();
  }

  private boolean isEmptyDatasetSchema(final AccelerationEntry entry) {
    return entry.getDescriptor().getContext().getDatasetSchema() == null
      || entry.getDescriptor().getContext().getDatasetSchema().getFieldList() == null
      || entry.getDescriptor().getContext().getDatasetSchema().getFieldList().isEmpty();
  }

  @Override
  public AccelerationEntry update(final AccelerationEntry entry)  {
    Preconditions.checkNotNull(entry.getDescriptor(), "descriptor is required");
    Preconditions.checkNotNull(entry.getDescriptor().getId(), "descriptor.id is required");
    AccelerationUtils.normalize(entry.getDescriptor());

    if (isEmptyDatasetSchema(entry)) {
      // Do not queue AnalysisStage in the background, but do it in the foreground.  This is because we need
      // the dataset schema to verify that the new updated layout makes sense.
      pipelineManager.analyzeInForeground(entry.getDescriptor());
      final Optional<AccelerationEntry> newEntryOptional = getAccelerationEntryById(entry.getDescriptor().getId());
      if (newEntryOptional.isPresent()) {
        // Since we have results from AnalysisStage, copy over the requested layouts.
        final AccelerationEntry newEntry = newEntryOptional.get();
        Preconditions.checkState(!isEmptyDatasetSchema(newEntry), "Failed to analyze dataset and get dataset schema");
        Preconditions.checkState(newEntry.getDescriptor().getVersion() >= entry.getDescriptor().getVersion(),
          String.format("Acceleration version %d after manual AnalysisStage should not be smaller that before %d", newEntry.getDescriptor().getVersion(), entry.getDescriptor().getVersion()));
        entry.getDescriptor().setVersion(newEntry.getDescriptor().getVersion());
        entry.getDescriptor().setContext(newEntry.getDescriptor().getContext());
      } else {
        throw UserException.validationError()
          .message(String.format("Could not find AccelerationEntry %s after manual AnalysisStage.", entry.getDescriptor().getId()))
          .build(logger);
      }
    }

    validateLayouts(entry);
    final AccelerationDescriptor descriptor = entry.getDescriptor();
    final boolean rawEnabled = descriptor.getRawLayouts().getEnabled();
    final boolean aggEnabled = descriptor.getAggregationLayouts().getEnabled();
    if (rawEnabled || aggEnabled) {
      descriptor.setState(AccelerationStateDescriptor.ENABLED);
    } else if (descriptor.getState() != AccelerationStateDescriptor.REQUESTED) {
      descriptor.setState(AccelerationStateDescriptor.DISABLED);
    }
    for (final LayoutDescriptor layout : Iterables.concat(
        descriptor.getRawLayouts().getLayoutList(),
        descriptor.getAggregationLayouts().getLayoutList())) {
      if (layout.getId() == null ) {
        layout.setId(AccelerationUtils.newRandomId());
      }
    }
    entryStore.save(entry);
    pipelineManager.update(descriptor);
    return entry;
  }

  private static Set<String> getFieldNames(final List<LayoutFieldDescriptor> descriptors) {
    return FluentIterable.from(selfOrEmpty(descriptors))
        .transform(new Function<LayoutFieldDescriptor, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final LayoutFieldDescriptor input) {
            return input.getName();
          }
        })
        .toSet();
  }

  private static boolean isEmpty(Collection<?> toCheck) {
    return toCheck == null || toCheck.isEmpty();
  }

  public static void validateLayouts(AccelerationEntry entry){
    {
      final LayoutContainerDescriptor agg = entry.getDescriptor().getAggregationLayouts();
      for(LayoutDescriptor descriptor : agg.getLayoutList()) {
        final LayoutDetailsDescriptor layout = descriptor.getDetails();
        final Set<String> dimensions = FluentIterable.from(selfOrEmpty(layout.getDimensionFieldList()))
            .transform(new Function<LayoutDimensionFieldDescriptor, String>() {
              @Nullable
              @Override
              public String apply(@Nullable final LayoutDimensionFieldDescriptor input) {
                return input.getName();
              }
            })
            .toSet();

        if (agg.getEnabled() && isEmpty(dimensions) && isEmpty(layout.getMeasureFieldList())) {
          throw UserException.validationError().message("At least one dimension or measure must be selected.").build(logger);
        }

        if (!isEmpty(layout.getDisplayFieldList())) {
          throw UserException.validationError().message("No display fields can be marked for aggregation acceleration.").build(logger);
        }

        if (!dimensions.containsAll(getFieldNames(selfOrEmpty(layout.getDistributionFieldList())))) {
          throw UserException.validationError().message("Distribution fields must also be marked as a dimension field.").build(logger);
        }

        if (!dimensions.containsAll(getFieldNames(selfOrEmpty(layout.getPartitionFieldList())))) {
          throw UserException.validationError().message("Partition fields must also be marked as a dimension field.").build(logger);
        }

        if (!dimensions.containsAll(getFieldNames(selfOrEmpty(layout.getSortFieldList())))) {
          throw UserException.validationError().message("Sort fields must also be marked as a dimension field.").build(logger);
        }

        if (!allDimensionScalar(entry.getDescriptor().getContext().getDatasetSchema(), layout.getDimensionFieldList())) {
          throw UserException.validationError().message("Dimension fields must be scalar fields.").build(logger);
        }
      }
    }

    {
      final LayoutContainerDescriptor raw = entry.getDescriptor().getRawLayouts();
      for(LayoutDescriptor descriptor : raw.getLayoutList()) {
        final LayoutDetailsDescriptor layout = descriptor.getDetails();
        final Set<LayoutFieldDescriptor> display = new HashSet<>(selfOrEmpty(layout.getDisplayFieldList()));

        if(raw.getEnabled() && isEmpty(display)){
          throw UserException.validationError().message("At least one field must be marked for display.").build(logger);
        }
        if(!isEmpty(layout.getDimensionFieldList())){
          throw UserException.validationError().message("No dimensions fields can be marked for a raw acceleration.").build(logger);
        }
        if(!isEmpty(layout.getMeasureFieldList())){
          throw UserException.validationError().message("No measure fields can be marked for a raw acceleration.").build(logger);
        }

        if(!display.containsAll(selfOrEmpty(layout.getDistributionFieldList()))){
          throw UserException.validationError().message("Distribution fields must also be marked for display.").build(logger);
        }
        if(!display.containsAll(selfOrEmpty(layout.getPartitionFieldList()))){
          throw UserException.validationError().message("Partition fields must also be marked for display.").build(logger);
        }

        if(!display.containsAll(selfOrEmpty(layout.getSortFieldList()))){
          throw UserException.validationError().message("Sort fields must also be marked for display.").build(logger);
        }

        if (!allScalar(entry.getDescriptor().getContext().getDatasetSchema(), layout.getDistributionFieldList())) {
          throw UserException.validationError().message("Distribution fields must be scalar fields.").build(logger);
        }

        if (!allScalar(entry.getDescriptor().getContext().getDatasetSchema(), layout.getPartitionFieldList())) {
          throw UserException.validationError().message("Partition fields must also be scalar fields.").build(logger);
        }

        if (!allScalar(entry.getDescriptor().getContext().getDatasetSchema(), layout.getSortFieldList())) {
          throw UserException.validationError().message("Sort fields must also be scalar fields.").build(logger);
        }
      }
    }
  }

  private static boolean allDimensionScalar(final RowType schema, final List<LayoutDimensionFieldDescriptor> fields) {
    if (schema == null) {
      throw UserException.validationError().message("Dataset schema cannot be null when validating acceleration layouts.").build(logger);
    }
    final Map<String, ViewFieldType> mapping = FluentIterable.from(selfOrEmpty(schema.getFieldList()))
      .uniqueIndex(new Function<ViewFieldType, String>() {
        @Nullable
        @Override
        public String apply(@Nullable final ViewFieldType input) {
          return input.getName();
        }
      });

    return FluentIterable.from(selfOrEmpty(fields))
      .allMatch(new Predicate<LayoutDimensionFieldDescriptor>() {
        @Override
        public boolean apply(@Nullable final LayoutDimensionFieldDescriptor input) {
          return Optional
            .fromNullable(mapping.get(input.getName()))
            .transform(new Function<ViewFieldType, Boolean>() {
              @Nullable
              @Override
              public Boolean apply(@Nullable final ViewFieldType input) {
                return isScalar(input.getType());
              }
            })
            .or(false);
        }
      });
  }

  private static boolean allScalar(final RowType schema, final List<LayoutFieldDescriptor> fields) {
    if (schema == null) {
      throw UserException.validationError().message("Dataset schema cannot be null when validating acceleration layouts.").build(logger);
    }
    final Map<String, ViewFieldType> mapping = FluentIterable.from(selfOrEmpty(schema.getFieldList()))
        .uniqueIndex(new Function<ViewFieldType, String>() {
          @Nullable
          @Override
          public String apply(@Nullable final ViewFieldType input) {
            return input.getName();
          }
        });

    return FluentIterable.from(selfOrEmpty(fields))
        .allMatch(new Predicate<LayoutFieldDescriptor>() {
          @Override
          public boolean apply(@Nullable final LayoutFieldDescriptor input) {
            return Optional
                .fromNullable(mapping.get(input.getName()))
                .transform(new Function<ViewFieldType, Boolean>() {
                  @Nullable
                  @Override
                  public Boolean apply(@Nullable final ViewFieldType input) {
                    return isScalar(input.getType());
                  }
                })
                .or(false);
          }
        });
  }

  private static boolean isScalar(final String type) {
    final SqlTypeName sqlType = SqlTypeName.valueOf(type);
    switch (sqlType) {
      case BIGINT:
      case BINARY:
      case BOOLEAN:
      case CHAR:
      case DATE:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INTEGER:
      case REAL:
      case TIME:
      case TIMESTAMP:
      case TINYINT:
      case VARBINARY:
      case VARCHAR:
        return true;
      default:
        return false;
    }
  }

  @Override
  public AccelerationEntry create(final Acceleration acceleration) {
    final AccelerationDescriptor descriptor = AccelerationMapper.toAccelerationDescriptor(acceleration);
    final AccelerationEntry entry = new AccelerationEntry().setDescriptor(descriptor);

    // ensure creation request is enqueued in pipeline, this will fail if entry already exists
    pipelineManager.create(acceleration);
    // then update user store
    entryStore.save(entry);
    return entry;
  }

  @Override
  public Iterable<Materialization> getMaterializations(final LayoutId layoutId) {
    return AccelerationUtils.getAllMaterializations(materializationStore.get(layoutId));
  }

  @Override
  public Optional<MaterializedLayout> getMaterializedLayout(final LayoutId layoutId) {
    return materializationStore.get(layoutId);
  }

  @Override
  public void remove(final AccelerationId id) {
    entryStore.remove(id);
    final Optional<Acceleration> acceleration = accelerationStore.get(id);
    if (acceleration.isPresent()) {
      for (final Layout layout : AccelerationUtils.getAllLayouts(acceleration.get())) {
        final Iterable<Materialization> materializations = FluentIterable
            .from(AccelerationUtils.getAllMaterializations(materializationStore.get(layout.getId())));
        for (final Materialization materialization : materializations) {
          if (materialization.getState() == MaterializationState.DONE) {
            dropMaterialization(materialization);
          }
        }
        materializationStore.remove(layout.getId());
      }
    }
    accelerationStore.remove(id);
  }

  @Override
  public void startBuildDependencyGraph() {
    executor.submit(RebuildRefreshGraph.of(this));
  }

  @Override
  public DependencyGraph buildRefreshDependencyGraph() {
    return buildDependencyGraph(getActiveLayouts());
  }

  @Override
  public SystemSettings getSettings() {
    final SystemOptionManager manager = contextProvider.get().getOptionManager();
    return new SystemSettings()
        .setLimit((int)manager.getOption(ExecConstants.ACCELERATION_LIMIT))
        .setAccelerateAggregation(manager.getOption(ExecConstants.ACCELERATION_AGGREGATION_ENABLED))
        .setAccelerateRaw(manager.getOption(ExecConstants.ACCELERATION_RAW_ENABLED))
        .setOrphanCleanupInterval(manager.getOption(ExecConstants.ACCELERATION_ORPHAN_CLEANUP_MILLISECONDS))
        .setLayoutRefreshMaxAttempts((int)manager.getOption(ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS));
  }

  @Override
  public void configure(final SystemSettings settings) {
    final SystemOptionManager manager = contextProvider.get().getOptionManager();
    manager.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.ACCELERATION_LIMIT.getOptionName(), settings.getLimit()));
    manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, ExecConstants.ACCELERATION_AGGREGATION_ENABLED.getOptionName(), settings.getAccelerateAggregation()));
    manager.setOption(OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, ExecConstants.ACCELERATION_RAW_ENABLED.getOptionName(), settings.getAccelerateRaw()));
    if (settings.getOrphanCleanupInterval() != null && getSettings().getOrphanCleanupInterval() != settings.getOrphanCleanupInterval()) {
      manager.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.ACCELERATION_ORPHAN_CLEANUP_MILLISECONDS.getOptionName(), settings.getOrphanCleanupInterval()));
      Cancellable cleanupTask = cleanupTaskRef.getAndSet(scheduleCleanupTask());
      cleanupTask.cancel();
    }
    if (settings.getLayoutRefreshMaxAttempts() != null) {
      manager.setOption(OptionValue.createLong(OptionValue.OptionType.SYSTEM, ExecConstants.LAYOUT_REFRESH_MAX_ATTEMPTS.getOptionName(), settings.getLayoutRefreshMaxAttempts()));
    }
  }

  @Override
  public boolean isPipelineCompletedOrNotStarted(AccelerationId id) {
    return pipelineManager.isPipelineCompletedOrNotStarted(id);
  }

  @Override
  public DeveloperAccelerationService developerService() {
    return devService;
  }

  /**
   * Compact the acceleration service by removing unnecessary materializations
   */
  void compact(final TemporalAmount gracePeriod) {
    for (final Acceleration acceleration : accelerationStore.find()) {
      final Iterable<Layout> layouts = AccelerationUtils.getAllLayouts(acceleration);
      for (final Layout layout : layouts) {
        final List<Materialization> toRemove = new ArrayList<>();
        Materialization toKeep = null;

        final Optional<MaterializedLayout> materializedLayoutOpt = materializationStore.get(layout.getId());
        if (!materializedLayoutOpt.isPresent()) {
          continue;
        }
        final MaterializedLayout materializedLayout = materializedLayoutOpt.get();
        final List<Materialization> materializations = Lists.newArrayList(selfOrEmpty(materializedLayout.getMaterializationList()));
        for (final Materialization materialization : materializations) {
          if (materialization.getState() != MaterializationState.DONE) {
            continue;
          }

          if (toKeep == null) {
            toKeep = materialization;
            continue;
          }

          Materialization toDrop;
          if (toKeep.getJob().getJobEnd() < materialization.getJob().getJobEnd()) {
            toDrop = toKeep;
            toKeep = materialization;
          } else {
            toDrop = materialization;
          }

          // Check if this dataset is in the grace period
          if (toDrop.getState() != MaterializationState.DELETED &&
              Instant.ofEpochMilli(toDrop.getJob().getJobEnd()).plus(gracePeriod).isAfter(Instant.now())) {
            continue;
          }

          toRemove.add(toDrop);
        }

        if (!toRemove.isEmpty()) {
          materializations.removeAll(toRemove);
          materializedLayout.setMaterializationList(materializations);
          try {
            materializationStore.save(materializedLayout);
          } catch (Throwable e) {
            // something went wrong, most likely another thread updated the layout
            logger.warn("Couldn't compact materialized layout {}", layout.getId(), e);
          }

          // now that we removed the materializations from the store we can go ahead and drop the corresponding
          // tables
          for (Materialization toDrop : toRemove) {
            dropMaterialization(toDrop);
          }
        }
      }

    }
  }

  /**
   * get a list of all active layouts.
   */
  @VisibleForTesting
  public Iterable<Layout> getActiveLayouts() {
    final Iterable<Acceleration> allActive = Iterables.concat(
        accelerationStore.getAllByIndex(AccelerationIndexKeys.ACCELERATION_STATE, AccelerationState.ENABLED.toString()),
        accelerationStore.getAllByIndex(AccelerationIndexKeys.ACCELERATION_STATE, AccelerationState.ENABLED_SYSTEM.toString())
        );

    return FluentIterable
        .from(allActive)
        .transformAndConcat(new Function<Acceleration, Iterable<Layout>>() {
          @Nullable
          @Override
          public Iterable<Layout> apply(@Nullable Acceleration acceleration) {
            return AccelerationUtils.allActiveLayouts(acceleration);
          }
        })
        .filter(new Predicate<Layout>() {
          @Override
          public boolean apply(Layout input) {
            Optional<MaterializedLayout>  materializedLayoutOpt = materializationStore.get(input.getId());
            if (materializedLayoutOpt.isPresent()) {
              MaterializedLayout materializedLayout = materializedLayoutOpt.get();
              return (materializedLayout.getState() != MaterializedLayoutState.FAILED);
            }
            return true;
          }
        });
  }

  private List<MaterializationDescriptor> getMaterializations(final boolean includeIncompleteDatasets) {
    final Iterable<Acceleration> accelerations = accelerationStore.find();
    final Set<DataPartition> activeHosts = getActiveHosts();

    return FluentIterable
        .from(accelerations)
        .transformAndConcat(new Function<Acceleration, Iterable<MaterializationDescriptor>>() {
          @Nullable
          @Override
          public Iterable<MaterializationDescriptor> apply(@Nullable final Acceleration acceleration) {
            return getMaterializations(acceleration, activeHosts, includeIncompleteDatasets);
          }
        })
        .toList();
  }

  /**
   * Returns effective materializations associated with the given acceleration
   */
  private Iterable<MaterializationDescriptor> getMaterializations(final Acceleration acceleration, final Set<DataPartition> activeHosts, final boolean includeIncompleteDatasets) {
    return FluentIterable
        .from(AccelerationUtils.allActiveLayouts(acceleration))
        .transformAndConcat(new Function<Layout, Iterable<MaterializationDescriptor>>() {
          @Nullable
          @Override
          public Iterable<MaterializationDescriptor> apply(@Nullable final Layout layout) {
            final Iterable<MaterializationWrapper> completed = getEffectiveMaterialization(layout, activeHosts, includeIncompleteDatasets);

            return FluentIterable
                .from(completed)
                .transform(new Function<MaterializationWrapper, MaterializationDescriptor>() {
                  @Nullable
                  @Override
                  public MaterializationDescriptor apply(@Nullable final MaterializationWrapper materializationInfo) {
                    return AccelerationUtils.getMaterializationDescriptor(acceleration, layout, materializationInfo.getMaterialization(), materializationInfo.isComplete());
                  }
                });
          }
        });
  }

  Optional<Materialization> getEffectiveMaterialization(final Layout layout) {
    MaterializationWrapper materializationInfo = Iterables.getFirst(getEffectiveMaterialization(layout, getActiveHosts(), false), null);
    if (materializationInfo != null) {
      return Optional.of(materializationInfo.getMaterialization());
    } else {
      return Optional.absent();
    }
  }

  private Set<DataPartition> getActiveHosts() {
    return Sets.newHashSet(Iterables.transform(contextProvider.get().getExecutors(), new Function<CoordinationProtos.NodeEndpoint, DataPartition>() {
      @Nullable
      @Override
      public DataPartition apply(@Nullable final CoordinationProtos.NodeEndpoint endpoint) {
        return new DataPartition(endpoint.getAddress());
      }
    }));
  }
  /**
   * get the latest materialization for a layout and the materialization state should be either DONE or RUNNING.
   * @param layout
   * @param states
   * @return
   */
  Optional<Materialization> getLatestMaterialization(final Layout layout) {
    MaterializationWrapper materializationInfo = Iterables.getFirst(getDoneAndRunningMaterializationsSortedByJobStartTime(layout), null);
    if (materializationInfo != null) {
      return Optional.of(materializationInfo.getMaterialization());
    } else {
      return Optional.absent();
    }
  }

  private Iterable<MaterializationWrapper> getDoneAndRunningMaterializationsSortedByJobStartTime(final Layout layout) {
    if (layout.getLogicalPlan() == null) {
      return ImmutableList.of();
    }

    final Set<MaterializationState> doneAndRunningStates =
        EnumSet.of(MaterializationState.DONE, MaterializationState.RUNNING);

    final List<Materialization> materializations = AccelerationUtils.getAllMaterializations(materializationStore.get(layout.getId()));

    List<Materialization> result = STARTTIME_ORDERING
        .greatestOf(FluentIterable
            .from(materializations)
            .filter(new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable final Materialization materialization) {
                if (!doneAndRunningStates.contains(materialization.getState())) {
                  return false;
                }
                if (materialization.getLayoutVersion() != layout.getVersion()) {
                  return false;
                }
                return true;
              }
            }), 1);

    return FluentIterable
        .from(result)
        .transform(new Function<Materialization, MaterializationWrapper>() {
          @Nullable
          @Override
          public MaterializationWrapper apply(@Nullable final Materialization materialization) {
            return new MaterializationWrapper(materialization, false);
          }
        });
  }

  private Iterable<MaterializationWrapper> getEffectiveMaterialization(final Layout layout, final Set<DataPartition> activeHosts, final boolean includeIncompleteDatasets) {
    if (layout.getLogicalPlan() == null) {
      return ImmutableList.of();
    }

    final List<Materialization> materializations = AccelerationUtils.getAllMaterializations(materializationStore.get(layout.getId()));

    final List<Materialization> expiredMaterializations = new ArrayList<>();
    final Set<Materialization> incompleteMaterializations = new HashSet<>();

    List<Materialization> result = COMPLETION_ORDERING
        .greatestOf(FluentIterable
            .from(materializations)
            .filter(new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable final Materialization materialization) {
                if (materialization.getState() != MaterializationState.DONE) {
                  return false;
                }
                if (materialization.getLayoutVersion() != layout.getVersion()) {
                  return false;
                }
                if (materialization.getExpiration() == null) {
                  logger.warn("No ttl found for layout: {}. Excluding materialization", layout.getId().getId());
                  return false;
                }
                return true;
              }
            })
            .filter(new Predicate<Materialization>() {
              @Override
              public boolean apply(@Nullable Materialization materialization) {
                List<DataPartition> partitionList = Optional.fromNullable(materialization.getPartitionList()).or(ImmutableList.<DataPartition>of());
                final ImmutableSet<DataPartition> partitions = ImmutableSet.copyOf(partitionList);
                if (!activeHosts.containsAll(partitions)) {
                  if (includeIncompleteDatasets) {
                    incompleteMaterializations.add(materialization);
                  } else {
                    return false;
                  }
                }
                long expiration = materialization.getExpiration();
                long currentTime = System.currentTimeMillis();
                if (currentTime > expiration) {
                  if (includeIncompleteDatasets) {
                    incompleteMaterializations.add(materialization);
                  } else {
                    expiredMaterializations.add(materialization);
                    return false;
                  }
                }
                return true;
              }
            }), 1);
    if (logger.isDebugEnabled() && !expiredMaterializations.isEmpty() && result.isEmpty()) {
      DateFormat formatter = new SimpleDateFormat();
      StringBuilder expiredString = new StringBuilder();
      for (Materialization expiredMaterialization : expiredMaterializations) {
        expiredString
        .append(" {id: ")
        .append(expiredMaterialization.getId().getId())
        .append(", exp: ")
        .append(formatter.format(new Date(expiredMaterialization.getExpiration())))
        .append("}");
      }
      logger.debug(
          "Excluding materializations that have expired:{}",
          expiredString.toString()
      );
    }
    return FluentIterable
        .from(result)
        .transform(new Function<Materialization, MaterializationWrapper>() {
          @Nullable
          @Override
          public MaterializationWrapper apply(@Nullable final Materialization materialization) {
            return new MaterializationWrapper(materialization, !incompleteMaterializations.contains(materialization));
          }
        });
  }

  private void startMaterializationPlugin() throws IOException, ExecutionSetupException {
    final MaterializationStoragePluginConfig config = new MaterializationStoragePluginConfig(true);
    contextProvider.get().getStorage().createOrUpdate(MATERIALIZATION_STORAGE_PLUGIN_NAME, config, null, true);
  }

  /**
   * Drops given materialization synchronously.
   */
  private void dropMaterialization(final Materialization materialization) {
    Acceleration acceleration = accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, materialization.getLayoutId().getId()).get();
    DatasetConfig dataset = namespaceService.get().findDatasetByUUID(acceleration.getId().getId());
    final DropTask task = new DropTask(acceleration, materialization, jobsService.get(), dataset);
    task.run();
  }

  private synchronized DependencyGraph buildDependencyGraph(Iterable<Layout> layouts) {
    logger.info("Started building the dependency graph.");
    Stopwatch stopWatch = Stopwatch.createStarted();

    final DependencyGraph graph = new DependencyGraph(this,
        jobsService.get(),
        namespaceService.get(),
        catalogService.get(),
        schedulerService.get(),
        acceleratorStoragePluginProvider.get().getStorageName(),
        materializationStore,
        executor,
        acceleratorStoragePluginProvider.get());

    Map<String, Layout> layoutMap = FluentIterable.from(layouts)
      .uniqueIndex(new Function<Layout, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Layout layout) {
          return layout.getId().getId();
        }
      });
    for (Layout layout : layoutMap.values()) {
      try {
        Optional<Acceleration> acceleration = accelerationStore.getByIndex(AccelerationIndexKeys.LAYOUT_ID, layout.getId().getId());
        if (acceleration.isPresent()) {
          DatasetConfig dataset = namespaceService.get().findDatasetByUUID(acceleration.get().getId().getId());
          if (dataset != null) {
            MaterializationPlanningTask task = materializationPlanningFactory.createTask(
                acceleratorStoragePluginProvider.get().getStorageName(),
                jobsService.get(), layout, namespaceService.get(), this, acceleration.get());
            executor.execute(task);
            DependencyNode vertex = new DependencyNode(layout);
            graph.addVertex(vertex);
            List<DependencyNode> dependencyNodes = task.getDependencies();
            for (DependencyNode node : dependencyNodes) {
              graph.addVertex(node);
              graph.addEdge(node, vertex);
            }
          }
        }
      } catch (ExecutionSetupException e) {
        AccelerationUtils.saveException(layout.getId(), e, materializationStore);
      }
    }
    graph.buildAndSchedule();
    logger.info("Finished building the dependency graph, took {} ms.", stopWatch.stop().elapsed(TimeUnit.MILLISECONDS));

    DependencyGraph old = dependencyGraph.getAndSet(graph);
    if (old != null) {
      old.close();
    }

    if (graph != null && logger.isDebugEnabled()) {
      logger.debug(graph.toString());
    }

    return graph;
  }

  @Override
  public void replan(LayoutId layoutId) {
    logger.info("Re-planning layout {}", layoutId);

    final Optional<AccelerationEntry> entry = getAccelerationEntryByLayoutId(layoutId);
    if (!entry.isPresent()) {
      logger.warn("couldn't find acceleration entry for layout {}, aborting re-planning", layoutId);
      return;
    }
    pipelineManager.replan(entry.get().getDescriptor());
  }

  private void cleanupOrphanedAccelerations() {
    final Iterable<Acceleration> accelerations = accelerationStore.find();
    for (final Acceleration acceleration : accelerations) {
      DatasetConfig dataset = namespaceService.get().findDatasetByUUID(acceleration.getId().getId());
      if (dataset == null) {
        // Remove the entry from the store
        logger.info("removing acceleration {} because dataset is removed", acceleration.getId().getId());
        remove(acceleration.getId());
      }
    }
  }

  private void updateDatasetPaths() {
    final Iterable<Acceleration> accelerations = accelerationStore.find();
    for (final Acceleration acceleration : accelerations) {
      DatasetConfig dataset = namespaceService.get().findDatasetByUUID(acceleration.getId().getId());
      if (dataset != null) {
        // if dataset path has changed, then update it on acceleration so that it gets indexed properly.
        if (!dataset.getFullPathList().equals(acceleration.getContext().getDataset().getFullPathList())) {
          logger.info("updating dataset path for acceleration {} from {} to {} .",
              acceleration.getId().getId(),
              acceleration.getContext().getDataset().getFullPathList(),
              dataset.getFullPathList() );
          acceleration.getContext().getDataset().setFullPathList(dataset.getFullPathList());
          accelerationStore.save(acceleration);
        }
      }
    }
  }

  private class DevService implements DeveloperAccelerationService {

    @Override
    public boolean isQueryAccelerationEnabled() {
      return queryAccelerationEnabled;
    }

    @Override
    public void enableQueryAcceleration(final boolean enabled) {
      queryAccelerationEnabled = enabled;
    }

    @Override
    public DependencyGraph getDependencyGraph() {
      return AccelerationServiceImpl.this.buildRefreshDependencyGraph();
    }

    @Override
    public MaterializationStore getMaterializationStore() {
      return materializationStore;
    }

    @Override
    public void triggerAccelerationBuild() {
      //TODO: trigger materialization
    }

    @Override
    public void triggerCompaction() {
      executor.submit(new GarbageCollectorTask(AccelerationServiceImpl.this, Duration.ZERO));
    }

    @Override
    public void clearAllAccelerations() {
      final Iterable<Acceleration> accelerations = accelerationStore.find();

      // Remove the entry from the store
      for (final Acceleration acceleration : accelerations) {
        remove(acceleration.getId());
      }
    }

    @Override
    public void removeOrphanedAccelerations() {
      cleanupOrphanedAccelerations();
    }

    @Override
    public void syncDatasetPaths() {
      updateDatasetPaths();
    }
  }

  /**
   * A function that keeps user store in sync with acceleration store.
   */
  private class EntryRefresher implements Function<AccelerationEntry, AccelerationEntry> {
    @Override
    public AccelerationEntry apply(final AccelerationEntry entry) {
      final AccelerationDescriptor descriptor = AccelerationUtils.normalize(entry.getDescriptor());
      final Optional<Acceleration> acceleration = pipelineManager.get(entry.getDescriptor().getId());
      if (acceleration.isPresent()) {
        final Acceleration current = acceleration.get();
        if (!AccelerationUtils.isPipelineDone(current)) {
          return entry;
        }

        // if pipeline is done then reconcile
        final AccelerationDescriptor newDescriptor = AccelerationMapper.toAccelerationDescriptor(current);
        newDescriptor.setVersion(descriptor.getVersion());
        entry.setDescriptor(newDescriptor);
        if (!newDescriptor.equals(descriptor)) {
          entryStore.save(entry);
        }
      }
      return entry;
    }
  }

  /**
   *  iterate through all accelerations
   *  check if dataset exists.
   *  If the dataset does not exist, then remove the acceleration
   */
  private class RemoveOrphanedAccelerations implements Runnable {
    @Override
    public void run() {
      cleanupOrphanedAccelerations();
    }
  }

  private class SyncDatasetPathsTask implements Runnable {
    @Override
    public void run() {
      updateDatasetPaths();
    }
  }
}
