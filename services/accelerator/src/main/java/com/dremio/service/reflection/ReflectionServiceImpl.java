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
package com.dremio.service.reflection;

import static com.dremio.common.utils.SqlUtils.quotedCompound;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_ENABLE_SUBSTITUTION;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_PERIODIC_WAKEUP_ONLY;
import static com.dremio.service.reflection.ReflectionUtils.computeDatasetHash;
import static com.dremio.service.reflection.ReflectionUtils.hasMissingPartitions;
import static com.dremio.service.scheduler.ScheduleUtils.scheduleForRunningOnceAt;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.base.Predicates.notNull;
import static java.time.Instant.ofEpochMilli;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.WakeupHandler;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.CachedMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.MaterializationExpander;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.serialization.kryo.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SessionOptionManagerImpl;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.reflection.MaterializationCache.CacheException;
import com.dremio.service.reflection.MaterializationCache.CacheHelper;
import com.dremio.service.reflection.MaterializationCache.CacheViewer;
import com.dremio.service.reflection.ReflectionService.BaseReflectionService;
import com.dremio.service.reflection.analysis.ReflectionAnalyzer;
import com.dremio.service.reflection.analysis.ReflectionAnalyzer.TableStats;
import com.dremio.service.reflection.analysis.ReflectionSuggester;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.refresh.RefreshHelper;
import com.dremio.service.reflection.refresh.RefreshStartHandler;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

/**
 * {@link ReflectionService} implementation
 */
public class ReflectionServiceImpl extends BaseReflectionService {
  private static final Logger logger = LoggerFactory.getLogger(ReflectionServiceImpl.class);

  public static final String LOCAL_TASK_LEADER_NAME = "reflectionsrefresh";

  public static final String ACCELERATOR_STORAGEPLUGIN_NAME = "__accelerator";

  private static final Comparator<MaterializationDescriptor> JOB_START_COMPARATOR = new Comparator<MaterializationDescriptor>() {
    @Override
    public int compare(MaterializationDescriptor m1, MaterializationDescriptor m2) {
      return Long.compare(m1.getJobStart(), m2.getJobStart());
    }
  };

  private static final MaterializationDescriptorFactory DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY = new MaterializationDescriptorFactory() {

    @Override
    public MaterializationDescriptor getMaterializationDescriptor(ReflectionGoal reflectionGoal,
        ReflectionEntry reflectionEntry, Materialization materialization, double originalCost) {
      return ReflectionUtils.getMaterializationDescriptor(reflectionGoal, reflectionEntry, materialization,
          originalCost);
    }
  };

  interface DescriptorCache {
    void invalidate(MaterializationId mId);
    void update(Materialization m) throws CacheException;
  }

  private MaterializationDescriptorProvider materializationDescriptorProvider;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<CatalogService> catalogService;
  private final Provider<SabotContext> sabotContext;
  private final Provider<ReflectionStatusService> reflectionStatusService;
  private final ReflectionSettings reflectionSettings;
  private final ExecutorService executorService;
  private final BufferAllocator allocator;

  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore internalStore;
  private final MaterializationStore materializationStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final DependenciesStore dependenciesStore;
  private final RefreshRequestsStore requestsStore;
  private final boolean isMaster;
  private final CacheHelperImpl cacheHelper = new CacheHelperImpl();
  /** set of all reflections that need to be updated next time the reflection manager wakes up */
  private final Set<ReflectionId> reflectionsToUpdate = Sets.newConcurrentHashSet();

  private DependencyManager dependencyManager;
  private MaterializationCache materializationCache;
  private WakeupHandler wakeupHandler;

  private final CacheViewer cacheViewer = new CacheViewer() {
    @Override
    public boolean isCached(MaterializationId id) {
      return !isCacheEnabled() || materializationCache.contains(id);
    }
  };

  /** dummy QueryContext used to create the SqlConverter, must be closed or we'll leak a ChildAllocator */
  private final Supplier<QueryContext> queryContext;
  private final Supplier<ExpansionHelper> expansionHelper;

  private final ReflectionValidator validator;

  private final MaterializationDescriptorFactory materializationDescriptorFactory;

  private ReflectionManager reflectionManager = null;

  public ReflectionServiceImpl(
    SabotConfig config,
    Provider<LegacyKVStoreProvider> storeProvider,
    Provider<SchedulerService> schedulerService,
    Provider<JobsService> jobsService,
    Provider<CatalogService> catalogService,
    final Provider<SabotContext> sabotContext,
    Provider<ReflectionStatusService> reflectionStatusService,
    ExecutorService executorService,
    boolean isMaster,
    BufferAllocator allocator) {
    this.schedulerService = Preconditions.checkNotNull(schedulerService, "scheduler service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "acceleration plugin required");
    this.reflectionStatusService = Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
    this.executorService = Preconditions.checkNotNull(executorService, "executor service required");
    this.namespaceService = new Provider<NamespaceService>() {
      @Override
      public NamespaceService get() {
        return sabotContext.get().getNamespaceService(SYSTEM_USERNAME);
      }
    };
    this.reflectionSettings = new ReflectionSettings(namespaceService, storeProvider);
    this.isMaster = isMaster;
    this.allocator = allocator.newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);

    userStore = new ReflectionGoalsStore(storeProvider);
    internalStore = new ReflectionEntriesStore(storeProvider);
    materializationStore = new MaterializationStore(storeProvider);
    externalReflectionStore = new ExternalReflectionStore(storeProvider);
    dependenciesStore = new DependenciesStore(storeProvider);
    requestsStore = new RefreshRequestsStore(storeProvider);

    this.queryContext = new Supplier<QueryContext>() {
      @Override
      public QueryContext get() {
        final UserSession session = systemSession(getOptionManager());
        return new QueryContext(session, sabotContext.get(), new AttemptId().toQueryId(),
            java.util.Optional.of(false));
      }
    };

    this.expansionHelper = new Supplier<ExpansionHelper>() {
      @Override
      public ExpansionHelper get() {
        return new ExpansionHelper(queryContext.get());
      }
    };

    this.validator = new ReflectionValidator(catalogService);
    this.materializationDescriptorFactory = config.getInstance(
        "dremio.reflection.materialization.descriptor.factory",
        MaterializationDescriptorFactory.class,
        DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY);
  }

  public MaterializationDescriptorProvider getMaterializationDescriptor() {
    return materializationDescriptorProvider;
  }

  @Override
  public void start() {
    this.materializationDescriptorProvider = new MaterializationDescriptorProviderImpl();

    // populate the materialization cache
    materializationCache = new MaterializationCache(cacheHelper, namespaceService.get(), reflectionStatusService.get());
    if (isCacheEnabled()) {
      // refresh the cache in-thread before any query gets planned
      materializationCache.refresh();
    } else {
      //expand all descriptors here and replan ones that fail
      for (Materialization m : getValidMaterializations()) {
        try {
          cacheHelper.expand(m);
        } catch (Exception e) {
          logger.warn("failed to expand materialization {}", m.getId().getId(), e);
        }
      }
    }

    // no automatic rePlan allowed after this point. Any failure to expand should cause the corresponding
    // materialization to be marked as failed
    cacheHelper.disableReplan();

    // only start the managers on the master node
    if (isMaster) {
      if (sabotContext.get().getDremioConfig().isMasterlessEnabled()) {
        final CountDownLatch wasRun = new CountDownLatch(1);
        final Cancellable task = schedulerService.get()
        .schedule(scheduleForRunningOnceAt(Instant.now(),
          LOCAL_TASK_LEADER_NAME, () -> {
              // set to null
              // this is needed if we encounter lost ZK connection
              // and we bounce back and force
              logger.info("Reflections cleanup");
              wakeupHandler = null;
              dependencyManager = null;
          }),
          () -> {
            masterInit();
            wasRun.countDown();
          });
        if (!task.isDone()) {
          try {
            wasRun.await();
          } catch (InterruptedException e) {
            logger.warn("InterruptedExeption while waiting for reflections initialization");
            Thread.currentThread().interrupt();
          }
        }
      } else {
        // if it is masterful mode just init
        masterInit();
      }
       // sends a wakeup event every reflection_manager_refresh_delay
      schedulerService.get().schedule(scheduleForRunningOnceAt(getNextRefreshTimeInMillis(), LOCAL_TASK_LEADER_NAME),
        new Runnable() {
          @Override
          public void run() {
            logger.debug("periodic refresh");
            wakeupManager("periodic refresh", true);
            schedulerService.get().schedule(scheduleForRunningOnceAt(getNextRefreshTimeInMillis(), LOCAL_TASK_LEADER_NAME), this);
          }
        }
      );
    }

    scheduleNextCacheRefresh(new CacheRefresher());
  }

  private void scheduleNextCacheRefresh(CacheRefresher refresher) {
    long cacheUpdateDelay;

    try {
      cacheUpdateDelay = getOptionManager().getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS);
    } catch (Exception e) {
      logger.warn("Failed to retrieve materialization cache refresh delay", e);
      cacheUpdateDelay = MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS.getDefault().getNumVal();
    }

    schedulerService.get().schedule(scheduleForRunningOnceAt(ofEpochMilli(System.currentTimeMillis() + cacheUpdateDelay)),
      refresher);
  }

  /**
   * Helper to keep together logic needed
   * for init on "master/distributed master" node
   */
  private void masterInit() {
    logger.info("Reflections masterInit");
    dependencyManager = new DependencyManager(reflectionSettings, materializationStore, internalStore, requestsStore, dependenciesStore);
    dependencyManager.start();

    final FileSystemPlugin accelerationPlugin = sabotContext.get().getCatalogService()
      .getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);

    this.reflectionManager = new ReflectionManager(
      sabotContext.get(),
      jobsService.get(),
      namespaceService.get(),
      getOptionManager(),
      userStore,
      internalStore,
      externalReflectionStore,
      materializationStore,
      dependencyManager,
      new DescriptorCacheImpl(),
      reflectionsToUpdate,
      this::wakeupManager,
      expansionHelper,
      allocator,
      accelerationPlugin.getConfig().getPath(),
      ReflectionGoalChecker.Instance,
      new RefreshStartHandler(
        namespaceService.get(),
        jobsService.get(),
        materializationStore,
        this::wakeupManager
      )
    );

    wakeupHandler = new WakeupHandler(executorService, reflectionManager);
  }

  @Override
  public void updateAccelerationBasePath() {
    if (reflectionManager != null) {
      final FileSystemPlugin accelerationPlugin = sabotContext.get().getCatalogService()
        .getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);
      reflectionManager.setAccelerationBasePath(accelerationPlugin.getConfig().getPath());
    }
  }

  public RefreshHelper getRefreshHelper() {
    return new RefreshHelper() {

      @Override
      public NamespaceService getNamespace() {
        return namespaceService.get();
      }

      @Override
      public ReflectionSettings getReflectionSettings() {
        return reflectionSettings;
      }

      @Override
      public MaterializationStore getMaterializationStore() {
        return materializationStore;
      }

    };
  }

  @Override
  public ExcludedReflectionsProvider getExcludedReflectionsProvider() {
    return dependencyManager.getExcludedReflectionsProvider();
  }

  private Instant getNextRefreshTimeInMillis() {
    return ofEpochMilli(System.currentTimeMillis() + getOptionManager().getOption(REFLECTION_MANAGER_REFRESH_DELAY_MILLIS));
  }

  @VisibleForTesting
  public void refreshCache() {
    if (isCacheEnabled()) {
      logger.debug("materialization cache refresh...");
      materializationCache.refresh();
    }
  }

  private boolean isCacheEnabled() {
    return getOptionManager().getOption(MATERIALIZATION_CACHE_ENABLED);
  }

  private static UserSession systemSession(OptionManager options) {
    final UserBitShared.UserCredentials credentials = UserBitShared.UserCredentials.newBuilder()
      .setUserName(SYSTEM_USERNAME)
      .build();
    return UserSession.Builder.newBuilder()
      .withSessionOptionManager(new SessionOptionManagerImpl(options.getOptionValidatorListing()), options)
      .withCredentials(credentials)
      .exposeInternalSources(true)
      .build();
  }

  /**
   * @return non expired DONE materializations that have at least one refresh
   */
  private Iterable<Materialization> getValidMaterializations() {
    final long now = System.currentTimeMillis();
    return Iterables.filter(materializationStore.getAllDoneWhen(now), new Predicate<Materialization>() {
      @Override
      public boolean apply(Materialization m) {
        return !Iterables.isEmpty(materializationStore.getRefreshes(m));
      }
    });
  }

  private Set<String> getActiveHosts() {
    return Sets.newHashSet(Iterables.transform(sabotContext.get().getExecutors(),
      new Function<CoordinationProtos.NodeEndpoint, String>() {
        @Override
        public String apply(final CoordinationProtos.NodeEndpoint endpoint) {
          return endpoint.getAddress();
        }
      }));
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  @Override
  public ReflectionId create(ReflectionGoal goal) {
    try {
      Preconditions.checkArgument(goal.getId() == null, "new reflection shouldn't have an ID");
      Preconditions.checkState(goal.getTag() == null, "new reflection shouldn't have a version");
      validator.validate(goal);
    } catch (Exception e) {
      throw UserException.validationError().message("Invalid reflection: %s", e.getMessage()).build(logger);
    }

    final ReflectionId reflectionId = new ReflectionId(UUID.randomUUID().toString());
    goal.setId(reflectionId);

    userStore.save(goal);
    logger.debug("create reflection goal {} (named {})", reflectionId.getId(), goal.getName());
    wakeupManager("reflection goal created");
    return reflectionId;
  }

  @Override
  public ReflectionId createExternalReflection(String name, List<String> dataset, List<String> targetDataset) {
    ReflectionId id = new ReflectionId(UUID.randomUUID().toString());
    try {
      DatasetConfig datasetConfig = namespaceService.get().getDataset(new NamespaceKey(dataset));
      if (datasetConfig == null) {
        throw UserException
          .validationError()
          .message(String.format("Dataset %s not found", quotedCompound(dataset)))
          .build(logger);
      }
      DatasetConfig targetDatasetConfig = namespaceService.get().getDataset(new NamespaceKey(targetDataset));
      if (targetDatasetConfig == null) {
        throw UserException
          .validationError()
          .message(String.format("Dataset %s not found", quotedCompound(targetDataset)))
          .build(logger);
      }
      ExternalReflection externalReflection = new ExternalReflection()
        .setId(id.getId())
        .setName(name)
        .setQueryDatasetId(datasetConfig.getId().getId())
        .setQueryDatasetHash(computeDatasetHash(datasetConfig, namespaceService.get(), true))
        .setTargetDatasetId(targetDatasetConfig.getId().getId())
        .setTargetDatasetHash(computeDatasetHash(targetDatasetConfig, namespaceService.get(), true));

      // check that we are able to get a MaterializationDescriptor before storing it
      MaterializationDescriptor descriptor = ReflectionUtils.getMaterializationDescriptor(externalReflection, namespaceService.get());
      if (descriptor == null) {
        throw UserException.validationError().message("Failed to validate external reflection " + name).build(logger);
      }

      // validate that we can convert to a materialization
      try (ExpansionHelper helper = expansionHelper.get()){
        descriptor.getMaterializationFor(helper.getConverter());
      }
      externalReflectionStore.addExternalReflection(externalReflection);
      return id;
    } catch (NamespaceException e) {
      throw UserException.validationError(e).build(logger);
    }
  }

  @Override
  public Optional<ExternalReflection> getExternalReflectionById(String id) {
    return Optional.fromNullable(externalReflectionStore.get(id));
  }

  @Override
  public Iterable<ExternalReflection> getExternalReflectionByDatasetPath(List<String> datasetPath) {
    DatasetConfig datasetConfig;
    try {
      datasetConfig = namespaceService.get().getDataset(new NamespaceKey(datasetPath));
    } catch (NamespaceException e) {
      throw UserException.validationError(e).build(logger);
    }
    if (datasetConfig == null) {
      throw UserException.validationError().message(String.format("Dataset %s not found", quotedCompound(datasetPath))).build(logger);
    }
    return externalReflectionStore.findByDatasetId(datasetConfig.getId().getId());
  }

  @Override
  public Iterable<ExternalReflection> getAllExternalReflections() {
    return externalReflectionStore.getExternalReflections();
  }

  @Override
  public void dropExternalReflection(String id) {
    externalReflectionStore.deleteExternalReflection(id);
  }

  @Override
  public void update(ReflectionGoal goal) {
    try {
      Preconditions.checkNotNull(goal, "reflection goal required");
      Preconditions.checkNotNull(goal.getId(), "reflection id required");
      Preconditions.checkNotNull(goal.getTag(), "reflection version required");

      Optional<ReflectionGoal> currentGoal = getGoal(goal.getId());
      // TODO: if there is no current goal, should we throw?
      if (currentGoal.isPresent()) {
        ReflectionGoal currentReflectionGoal = currentGoal.get();
        if (currentReflectionGoal.getState() == ReflectionGoalState.DELETED) {
          throw UserException.validationError().message("Cannot update a deleted reflection").build(logger);
        }

        if (currentReflectionGoal.getType() != goal.getType()) {
          throw UserException.validationError().message("Cannot change the type of an existing reflection").build(logger);
        }

        if (!currentReflectionGoal.getDatasetId().equals(goal.getDatasetId())) {
          throw UserException.validationError().message("Cannot change the dataset id of an existing reflection").build(logger);
        }
      }

      if (goal.getState() == ReflectionGoalState.ENABLED) {
        validator.validate(goal);
      }
    } catch (Exception e) {
      throw UserException.validationError().message("Invalid reflection: %s", e.getMessage()).build(logger);
    }

    userStore.save(goal);
    wakeupManager("reflection goal updated");
  }

  @Override
  public Optional<ReflectionEntry> getEntry(ReflectionId reflectionId) {
    return Optional.fromNullable(internalStore.get(reflectionId));
  }

  @Override
  public Optional<ReflectionGoal> getGoal(ReflectionId reflectionId) {
    final ReflectionGoal goal = userStore.get(reflectionId);
    if (goal == null || goal.getState() == ReflectionGoalState.DELETED) {
      return Optional.absent();
    }
    return Optional.of(goal);
  }

  @Override
  public MaterializationMetrics getMetrics(Materialization materialization) {
    return materializationStore.getMetrics(materialization);
  }

  @Override
  public void clearAll() {
    final Iterable<ReflectionGoal> reflections = userStore.getAll();
    for (ReflectionGoal goal : reflections) {
      if (goal.getState() != ReflectionGoalState.DELETED) {
        userStore.save(goal.setState(ReflectionGoalState.DELETED));
      }
    }
  }

  @Override
  public Iterable<DependencyEntry> getDependencies(ReflectionId reflectionId) {
    if (dependencyManager != null) {
      return dependencyManager.getDependencies(reflectionId);
    }
    return super.getDependencies(reflectionId);
  }

  @Override
  public Iterable<AccelerationListManager.DependencyInfo> getReflectionDependencies() {
    final Iterable<ReflectionGoal> goalReflections = getAllReflections();
    final List<AccelerationListManager.DependencyInfo> reflectionDependencies = new LinkedList<>();
    for(ReflectionGoal goal : goalReflections) {
      ReflectionId goalId = goal.getId();
      final List<DependencyEntry> dependencyEntries = dependencyManager.getDependencies(goalId);
      for(DependencyEntry entry: dependencyEntries) {
        reflectionDependencies.add(new AccelerationListManager.DependencyInfo(
          goalId.getId(),
          entry.getId(),
          entry.getType().toString(),
          entry.getPath().toString()
        ));
      }
    }
    return reflectionDependencies;
  }

  @Override
  public Iterable<ReflectionGoal> getAllReflections() {
    return ReflectionUtils.getAllReflections(userStore);
  }

  @Override
  public long getTotalReflectionSize(ReflectionId reflectionId) {
    Iterable<Refresh> refreshes = materializationStore.getRefreshesByReflectionId(reflectionId);
    long size = 0;
    for (Refresh refresh : refreshes) {
      if (refresh.getMetrics() != null) {
        size += Optional.fromNullable(refresh.getMetrics().getFootprint()).or(0L);
      }
    }
    return size;
  }

  @Override
  public Iterable<ReflectionGoal> getReflectionsByDatasetPath(NamespaceKey path) {
    try {
      DatasetConfig config = namespaceService.get().getDataset(path);
      return getReflectionsByDatasetId(config.getId().getId());
    }catch(NamespaceException ex) {
      throw Throwables.propagate(ex);
    }
  }

  @VisibleForTesting
  public Iterable<ReflectionGoal> getReflectionGoals(final NamespaceKey path, final String reflectionName) {
    try {
      DatasetConfig config = namespaceService.get().getDataset(path);
      return FluentIterable.from(getReflectionsByDatasetId(config.getId().getId())).filter(new Predicate<ReflectionGoal>() {

        @Override
        public boolean apply(ReflectionGoal input) {
          return reflectionName.equals(input.getName());
        }});
    }catch(NamespaceException ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public Iterable<ReflectionGoal> getReflectionsByDatasetId(String datasetid) {
    return userStore.getByDatasetId(datasetid);
  }

  @Override
  public int getEnabledReflectionCountForDataset(String datasetid) {
    return userStore.getEnabledByDatasetId(datasetid);
  }

  @Override
  public boolean doesReflectionHaveAnyMaterializationDone(ReflectionId reflectionId) {
    return materializationStore.getLastMaterializationDone(reflectionId) != null;
  }

  @Override
  public Materialization getLastDoneMaterialization(ReflectionId reflectionId) {
    final Materialization materialization = materializationStore.getLastMaterializationDone(reflectionId);
    if (materialization == null) {
      throw new NotFoundException("materialization not found for " + reflectionId.getId());
    }
    return materialization;
  }

  @Override
  public void setSubstitutionEnabled(boolean enable) {
    getOptionManager().setOption(OptionValue.createBoolean(SYSTEM, REFLECTION_ENABLE_SUBSTITUTION.getOptionName(), enable));
  }

  @Override
  public boolean isSubstitutionEnabled() {
    return getOptionManager().getOption(REFLECTION_ENABLE_SUBSTITUTION);
  }

  @Override
  public Iterable<Materialization> getMaterializations(ReflectionId reflectionId) {
    return materializationStore.find(reflectionId);
  }

  @VisibleForTesting
  public void remove(ReflectionId id) {
    Optional<ReflectionGoal> goal = getGoal(id);
    if(goal.isPresent() && goal.get().getState() != ReflectionGoalState.DELETED) {
      update(goal.get().setState(ReflectionGoalState.DELETED));
    }
  }

  @Override
  public void remove(ReflectionGoal goal) {
    update(goal.setState(ReflectionGoalState.DELETED));
  }

  @Override
  public Optional<Materialization> getMaterialization(MaterializationId materializationId) {
    return Optional.fromNullable(materializationStore.get(materializationId));
  }

  @Override
  public Materialization getLastMaterialization(ReflectionId reflectionId) {
    return materializationStore.getLastMaterialization(reflectionId);
  }

  @Override
  public Iterable<Refresh> getRefreshes(Materialization materialization) {
    return materializationStore.getRefreshes(materialization);
  }

  @Override
  public List<ReflectionGoal> getRecommendedReflections(String datasetId) {
    DatasetConfig datasetConfig = namespaceService.get().findDatasetByUUID(datasetId);

    if (datasetConfig == null) {
      throw new NotFoundException("Dataset not found");
    }

    ReflectionAnalyzer analyzer = new ReflectionAnalyzer(jobsService.get(), catalogService.get(), allocator);

    TableStats tableStats = analyzer.analyze(new NamespaceKey(datasetConfig.getFullPathList()));

    ReflectionSuggester suggester = new ReflectionSuggester(datasetConfig, tableStats);

    return suggester.getReflectionGoals();
  }

  @Override
  public ReflectionSettings getReflectionSettings() {
    return reflectionSettings;
  }

  @Override
  public void requestRefresh(String datasetId) {
    logger.debug("refresh requested on {}", datasetId);
    RefreshRequest request = requestsStore.get(datasetId);
    if (request == null) {
      request = new RefreshRequest()
        .setDatasetId(datasetId)
        .setRequestedAt(0L);
    }
    request.setRequestedAt(Math.max(System.currentTimeMillis(), request.getRequestedAt()));
    requestsStore.save(datasetId, request);
    wakeupManager("refresh request for dataset " + datasetId);
  }

  @Override
  public Future<?> wakeupManager(String reason) {
    return wakeupManager(reason,false);
  }

  @Override
  public Provider<CacheViewer> getCacheViewerProvider() {
    return new Provider<CacheViewer>() {
      @Override
      public CacheViewer get() {
        return cacheViewer;
      }
    };
  }

  @Override
  public long getReflectionSize(ReflectionId reflectionId) {
    if (doesReflectionHaveAnyMaterializationDone(reflectionId)) {
      return getMetrics(getLastDoneMaterialization(reflectionId)).getFootprint();
    }

    return -1;
  }

  @Override
  public boolean isReflectionIncremental(ReflectionId reflectionId) {
    Optional<ReflectionEntry> entry = getEntry(reflectionId);
    if (entry.isPresent()) {
      return entry.get().getRefreshMethod() == RefreshMethod.INCREMENTAL;
    }

    return false;
  }

  private OptionManager getOptionManager() {
    return sabotContext.get().getOptionManager();
  }

  private Future<?> wakeupManager(String reason, boolean periodic) {
    final boolean periodicWakeupOnly = getOptionManager().getOption(REFLECTION_PERIODIC_WAKEUP_ONLY);
    if (wakeupHandler != null && (!periodicWakeupOnly || periodic)) {
      return wakeupHandler.handle(reason);
    }
    return CompletableFuture.completedFuture(null);
  }

  private MaterializationDescriptor getDescriptor(Materialization materialization) throws CacheException {
    final ReflectionGoal goal = userStore.get(materialization.getReflectionId());
    if (!ReflectionGoalChecker.checkGoal(goal, materialization)) {
      // reflection goal changed and corresponding materialization is no longer valid
      throw new CacheException("Unable to expand materialization " + materialization.getId().getId() +
        " as it no longer matches its reflection goal");
    }

    final ReflectionEntry entry = internalStore.get(materialization.getReflectionId());

    MaterializationMetrics metrics = materializationStore.getMetrics(materialization);

    return materializationDescriptorFactory.getMaterializationDescriptor(
        goal,
        entry,
        materialization,
        metrics.getOriginalCost());
  }

  private final class MaterializationDescriptorProviderImpl implements MaterializationDescriptorProvider {

    @Override
    public List<MaterializationDescriptor> get() {

      if (!isSubstitutionEnabled()) {
        return Collections.emptyList();
      }

      final long currentTime = System.currentTimeMillis();
      final Set<String> activeHosts = getActiveHosts();
      FluentIterable<MaterializationDescriptor> descriptors;
      if (isCacheEnabled()) {
        descriptors = FluentIterable.from(materializationCache.getAll())
          .filter(new Predicate<MaterializationDescriptor>() {
            @Override
              public boolean apply(MaterializationDescriptor descriptor) {
                return descriptor.getExpirationTimestamp() > currentTime && activeHosts.containsAll(descriptor.getPartition());
              }
            }
          );
      } else {
        descriptors = FluentIterable.from(getValidMaterializations())
          .filter(new Predicate<Materialization>() {
            @Override
            public boolean apply(Materialization m) {
              return !hasMissingPartitions(m.getPartitionList(), activeHosts);
            }
          }).transform(new Function<Materialization, MaterializationDescriptor>() {
            @Override
            public MaterializationDescriptor apply(Materialization m) {
              try {
                // we don't need to expand here, but we do so to be able to update reflections when we fail to expand
                // their materializations
                return cacheHelper.expand(m);
              } catch (Exception e) {
                logger.warn("couldn't expand materialization {}", m.getId().getId(), e);
                return null;
              }
            }
          });

        Iterable<MaterializationDescriptor> externalDescriptors = FluentIterable.from(getAllExternalReflections())
          .transform(new Function<ExternalReflection, MaterializationDescriptor>() {
            @Nullable
            @Override
            public MaterializationDescriptor apply(ExternalReflection externalReflection) {
              try {
                return ReflectionUtils.getMaterializationDescriptor(externalReflection, namespaceService.get());
              } catch (Exception e) {
                logger.debug("failed to get MaterializationDescriptor for external reflection {}", externalReflection.getName());
                return null;
              }
            }
          });

        descriptors = descriptors
          .append(externalDescriptors)
          .filter(notNull());
      }

      if (Iterables.isEmpty(descriptors)) {
        return Collections.emptyList();
      }

      // group the materializations by reflectionId and keep one by reflection
      final ImmutableListMultimap<ReflectionId, MaterializationDescriptor> descriptorMap =
        descriptors.index(new Function<MaterializationDescriptor, ReflectionId>() {
          @Override
          public ReflectionId apply(MaterializationDescriptor m) {
            return new ReflectionId(m.getLayoutId());
          }
        });
      // for each reflection, get latest materialization
      final Ordering<MaterializationDescriptor> ordering = Ordering.from(JOB_START_COMPARATOR);
      return FluentIterable.from(descriptorMap.keySet())
        .transform(new Function<ReflectionId, MaterializationDescriptor>() {
          @Override
          public MaterializationDescriptor apply(ReflectionId reflectionId) {
            return ordering.max(descriptorMap.get(reflectionId));
          }
        })
        .toList();
    }

    private boolean isDefaultReflectionEnabled(NamespaceKey path) {
      try {
        DatasetConfig datasetConfig = namespaceService.get().getDataset(path);
        if (!datasetConfig.getType().equals(DatasetType.VIRTUAL_DATASET) || datasetConfig.getVirtualDataset() == null) {
          return false;
        }
        return Optional.fromNullable(datasetConfig.getVirtualDataset().getDefaultReflectionEnabled()).or(true);
      } catch (NamespaceException e) {
        logger.debug("Dataset {} not found", path);
        return false;
      }
    }

    @Override
    public java.util.Optional<MaterializationDescriptor> getDefaultRawMaterialization(NamespaceKey path, List<String> vdsFields) {
      if (isSubstitutionEnabled()) {
        try {
          for (ReflectionGoal goal : getReflectionsByDatasetPath(path)) {
            if (goal.getType() == ReflectionType.RAW) {
              List<String> displayFields = goal.getDetails().getDisplayFieldList().stream().map(ReflectionField::getName).sorted().collect(Collectors.toList());
              if (displayFields.equals(vdsFields)) {
                final long currentTime = System.currentTimeMillis();
                final Set<String> activeHosts = getActiveHosts();
                Set<CachedMaterializationDescriptor> expandedMaterializations;
                Stream<Materialization> materializationStream = Stream.of(materializationStore.getLastMaterializationDone(goal.getId()))
                  .filter(Objects::nonNull)
                  .filter((Predicate<Materialization>) m -> !Iterables.isEmpty(materializationStore.getRefreshes(m)))
                  .filter(m -> !hasMissingPartitions(m.getPartitionList(), activeHosts));
                if (isCacheEnabled()) {
                  expandedMaterializations = materializationStream.map(m -> (CachedMaterializationDescriptor) materializationCache.get(m.getId()))
                    .filter(Objects::nonNull)
                    .filter((Predicate<MaterializationDescriptor>) descriptor -> descriptor.getExpirationTimestamp() > currentTime && activeHosts.containsAll(descriptor.getPartition()))
                    .collect(Collectors.toSet());
                } else {
                  expandedMaterializations = materializationStream.map((Function<Materialization, CachedMaterializationDescriptor>) m -> {
                    try {
                      return cacheHelper.expand(m);
                    } catch (Exception e) {
                      logger.warn("Couldn't expand materialization {}", m.getId().getId(), e);
                      return null;
                    }
                  })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());
                }
                if (!expandedMaterializations.isEmpty()) {
                  // We expect the feature to be enabled in the majority of cases. So we want to wait until
                  // we know that there are default reflections available for the dataset. This way we avoid
                  // hitting the namspace for every dataset in the tree, even if the dataset doesn't have any
                  // reflections
                  if (!isDefaultReflectionEnabled(path)) {
                    return java.util.Optional.empty();
                  }
                  CachedMaterializationDescriptor desc = expandedMaterializations.iterator().next();
                  if (!(desc.getMaterialization().getIncrementalUpdateSettings().isIncremental() && desc.getMaterialization().hasAgg())) {
                    // Do not apply default reflections for incremental refresh if there is an agg in the query plan
                    return java.util.Optional.of(desc);
                  }
                }
              }
            }
          }
        } catch (Exception ex) {
          if (ex.getCause() instanceof NamespaceNotFoundException) {
            logger.debug("Error while expanding view with path {}: {}", path, ex.getMessage());
          } else {
            Throwables.propagate(ex);
          }
        }
      }
      return java.util.Optional.empty();
    }
  }

  private final class CacheHelperImpl implements CacheHelper {
    private boolean rePlanIfNecessary = true;

    void disableReplan() {
      rePlanIfNecessary = false;
    }

    @Override
    public Iterable<Materialization> getValidMaterializations() {
      return ReflectionServiceImpl.this.getValidMaterializations();
    }

    @Override
    public Iterable<ExternalReflection> getExternalReflections() {
      return ReflectionServiceImpl.this.getAllExternalReflections();
    }

    @Override
    public MaterializationDescriptor getDescriptor(ExternalReflection externalReflection) throws CacheException {
      try {
        return ReflectionUtils.getMaterializationDescriptor(externalReflection, namespaceService.get());
      } catch (NamespaceException e) {
        throw new CacheException("Unable to get descriptor for " + externalReflection.getName());
      }
    }

    @Override
    public CachedMaterializationDescriptor expand(Materialization materialization) throws CacheException {
      final MaterializationDescriptor descriptor = ReflectionServiceImpl.this.getDescriptor(materialization);
      final DremioMaterialization expanded = expand(descriptor);
      if (expanded == null) {
        return null;
      }
      return new CachedMaterializationDescriptor(descriptor, expanded);
    }

    @Override
    public DremioMaterialization expand(MaterializationDescriptor descriptor) {
      final ReflectionId rId = new ReflectionId(descriptor.getLayoutId());
      if (reflectionsToUpdate.contains(rId)) {
        // reflection already scheduled for update
        return null;
      }

      // get a new converter for each materialization. This ensures that we
      // always index flattens from zero. This is a partial fix for flatten
      // matching. We should really do a better job in matching.
      try (ExpansionHelper helper = expansionHelper.get()) {
        return descriptor.getMaterializationFor(helper.getConverter());
      } catch (KryoLogicalPlanSerializers.KryoDeserializationException e) {
        final UserException uex = ErrorHelper.findWrappedCause(e, UserException.class);
        if (uex != null && uex.getErrorType() == UserBitShared.DremioPBError.ErrorType.SOURCE_BAD_STATE) {
          logger.debug("failed to expand materialization descriptor {}/{} because source is down, skip for now",
            descriptor.getLayoutId(), descriptor.getMaterializationId(), uex);
          return null;
        }

        if (!rePlanIfNecessary) {
          // replan not allowed, just rethrow the exception
          throw e;
        }

        logger.debug("failed to expand materialization descriptor {}/{}. Associated reflection will be scheduled for update",
          descriptor.getLayoutId(), descriptor.getMaterializationId(), e);
      } catch (MaterializationExpander.ExpansionException e) {
        if (!rePlanIfNecessary) {
          // replan not allowed, just rethrow the exception
          throw e;
        }

        logger.debug("failed to expand materialization descriptor {}/{}. Associated reflection will be scheduled for update",
          descriptor.getLayoutId(), descriptor.getMaterializationId(), e);
      }

      // mark reflection for update
      reflectionsToUpdate.add(new ReflectionId(descriptor.getLayoutId()));
      wakeupManager("failed to expand materialization"); // we should wake up the manager to update the reflection
      return null;
    }
  }

  public void resetCache() {
    materializationCache.resetCache();
  }

  public ReflectionManager getReflectionManager() {
    return reflectionManager;
  }

  private final class DescriptorCacheImpl implements DescriptorCache {
    @Override
    public void invalidate(MaterializationId mId) {
      if (isCacheEnabled()) {
        logger.debug("invalidating cache entry for {}", mId.getId());
        materializationCache.invalidate(mId);
      }
    }

    @Override
    public void update(Materialization m) throws CacheException {
      if (isCacheEnabled()) {
        logger.debug("updating cache entry for {}", m.getId().getId());
        materializationCache.update(m);
      }
    }
  }

  private final class CacheRefresher implements Runnable {
    @Override
    public void run() {
      try {
        refreshCache();
      } finally {
        scheduleNextCacheRefresh(this);
      }
    }
  }

  /**
   * Materialization expansion helper that takes care of releasing the query context when closed.
   * Caller must close the helper when done using the converter
   */
  public static class ExpansionHelper implements AutoCloseable {
    private final QueryContext context;
    private final SqlConverter converter;

    ExpansionHelper(QueryContext context) {
      this.context = Preconditions.checkNotNull(context, "query context required");
      converter = new SqlConverter(
        context.getPlannerSettings(),
        context.getOperatorTable(),
        context,
        MaterializationDescriptorProvider.EMPTY,
        context.getFunctionRegistry(),
        context.getSession(),
        AbstractAttemptObserver.NOOP,
        context.getCatalog(),
        context.getSubstitutionProviderFactory(),
        context.getConfig(),
        context.getScanResult());
    }

    public SqlConverter getConverter() {
      return converter;
    }

    @Override
    public void close() {
      AutoCloseables.closeNoChecked(context);
    }
  }
}
