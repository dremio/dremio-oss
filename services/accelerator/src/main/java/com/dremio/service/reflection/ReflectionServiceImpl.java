/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import static com.dremio.exec.server.options.OptionValue.OptionType.SYSTEM;
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
import static org.threeten.bp.Instant.ofEpochMilli;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.threeten.bp.Instant;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.ErrorHelper;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.acceleration.KryoLogicalPlanSerializers;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.sql.CachedMaterializationDescriptor;
import com.dremio.exec.planner.sql.DremioRelOptMaterialization;
import com.dremio.exec.planner.sql.MaterializationDescriptor;
import com.dremio.exec.planner.sql.MaterializationExpander;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.exec.work.AttemptId;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.BindingCreator;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
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
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.reflection.store.RefreshRequestsStore;
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
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReflectionServiceImpl.class);

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

  private final Provider<SchedulerService> schedulerService;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<CatalogService> catalogService;
  private final Provider<FileSystemPlugin> accelerationPlugin;
  private final Provider<SabotContext> sabotContext;
  private final Provider<ReflectionStatusService> reflectionStatusService;
  private final ReflectionSettings reflectionSettings;
  private final ExecutorService executorService;

  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore internalStore;
  private final MaterializationStore materializationStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final DependenciesStore dependenciesStore;
  private final RefreshRequestsStore requestsStore;
  private final BindingCreator bindingCreator;
  private final boolean isMaster;
  private final CacheHelper cacheHelper = new CacheHelperImpl();
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
  public ReflectionServiceImpl(
    SabotConfig config,
    Provider<KVStoreProvider> storeProvider,
    Provider<SchedulerService> schedulerService,
    Provider<JobsService> jobsService,
    Provider<CatalogService> catalogService,
    Provider<FileSystemPlugin> accelerationPlugin,
    final Provider<SabotContext> sabotContext,
    Provider<ReflectionStatusService> reflectionStatusService,
    ExecutorService executorService,
    BindingCreator bindingCreator,
    boolean isMaster) {
    this.schedulerService = Preconditions.checkNotNull(schedulerService, "scheduler service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.accelerationPlugin = Preconditions.checkNotNull(accelerationPlugin, "acceleration plugin required");
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "acceleration plugin required");
    this.reflectionStatusService = Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
    this.executorService = Preconditions.checkNotNull(executorService, "executor service required");
    this.bindingCreator = Preconditions.checkNotNull(bindingCreator, "binding creator required");
    this.namespaceService = new Provider<NamespaceService>() {
      @Override
      public NamespaceService get() {
        return sabotContext.get().getNamespaceService(SYSTEM_USERNAME);
      }
    };
    this.reflectionSettings = new ReflectionSettings(namespaceService, storeProvider);
    this.isMaster = isMaster;

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
        return new QueryContext(session, sabotContext.get(), new AttemptId().toQueryId());
      }
    };

    this.expansionHelper = new Supplier<ExpansionHelper>() {
      @Override
      public ExpansionHelper get() {
        return new ExpansionHelper(queryContext.get());
      }
    };

    this.validator = new ReflectionValidator(namespaceService, catalogService);
    this.materializationDescriptorFactory = config.getInstance(
        "dremio.reflection.materialization.descriptor.factory",
        MaterializationDescriptorFactory.class,
        DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY);
  }

  @Override
  public void start() {

    bindingCreator.replace(MaterializationDescriptorProvider.class, new MaterializationDescriptorProviderImpl());

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

    // only start the managers on the master node
    if (isMaster) {

      dependencyManager = new DependencyManager(reflectionSettings, materializationStore, internalStore, requestsStore, dependenciesStore);
      dependencyManager.start();

      final ReflectionManager manager = new ReflectionManager(
        jobsService.get(),
        namespaceService.get(),
        getOptionManager(),
        userStore,
        internalStore,
        externalReflectionStore,
        materializationStore,
        accelerationPlugin.get(),
        dependencyManager,
        new DescriptorCacheImpl(),
        reflectionsToUpdate,
        new ReflectionManager.WakeUpCallback() {
          @Override
          public void wakeup(String reason) {
            wakeupManager(reason);
          }
        },
        expansionHelper);
      wakeupHandler = new WakeupHandler(executorService, manager);

      // sends a wakeup event every reflection_manager_refresh_delay
      schedulerService.get().schedule(scheduleForRunningOnceAt(getNextRefreshTimeInMillis()),
        new Runnable() {
          @Override
          public void run() {
            wakeupManager("periodic refresh", true);
            schedulerService.get().schedule(scheduleForRunningOnceAt(getNextRefreshTimeInMillis()), this);
          }
        }
      );
    }
    bindingCreator.replace(AccelerationManager.class, new AccelerationManagerImpl(this, namespaceService.get()));

    final long cacheUpdateDelay = getOptionManager().getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS);
    schedulerService.get().schedule(scheduleForRunningOnceAt(ofEpochMilli(System.currentTimeMillis() + cacheUpdateDelay)),
      new CacheRefresher());
  }

  RefreshHelper getRefreshHelper() {
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
    if (dependencyManager != null) {
      return dependencyManager.getExcludedReflectionsProvider();
    }
    return super.getExcludedReflectionsProvider();
  }

  private Instant getNextRefreshTimeInMillis() {
    return ofEpochMilli(System.currentTimeMillis() + getOptionManager().getOption(REFLECTION_MANAGER_REFRESH_DELAY_MILLIS));
  }

  @VisibleForTesting
  void refreshCache() {
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
      .withCredentials(credentials)
      .withOptionManager(options)
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
  }

  @Override
  public ReflectionId create(ReflectionGoal goal) {
    try {
      Preconditions.checkArgument(goal.getId() == null, "new reflection shouldn't have an ID");
      Preconditions.checkState(goal.getVersion() == null, "new reflection shouldn't have a version");
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
        .setQueryDatasetHash(computeDatasetHash(datasetConfig, namespaceService.get()))
        .setTargetDatasetId(targetDatasetConfig.getId().getId())
        .setTargetDatasetHash(computeDatasetHash(targetDatasetConfig, namespaceService.get()));

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
      Preconditions.checkNotNull(goal.getVersion(), "reflection version required");

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
    logger.warn("Clearing all reflections");
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

  @Override
  public void remove(ReflectionGoal goal) {
    update(goal.setState(ReflectionGoalState.DELETED));
  }

  @Override
  public Optional<Materialization> getMaterialization(MaterializationId materializationId) {
    return Optional.fromNullable(materializationStore.get(materializationId));
  }

  @Override
  public List<ReflectionGoal> getRecommendedReflections(String datasetId) {
    DatasetConfig datasetConfig = namespaceService.get().findDatasetByUUID(datasetId);

    if (datasetConfig == null) {
      throw new NotFoundException("Dataset not found");
    }

    ReflectionAnalyzer analyzer = new ReflectionAnalyzer(jobsService.get(), catalogService.get());

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
  public void wakeupManager(String reason) {
    wakeupManager(reason,false);
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

  private SystemOptionManager getOptionManager() {
    return sabotContext.get().getOptionManager();
  }

  private void wakeupManager(String reason, boolean periodic) {
    final boolean periodicWakeupOnly = getOptionManager().getOption(REFLECTION_PERIODIC_WAKEUP_ONLY);
    if (wakeupHandler != null && (!periodicWakeupOnly || periodic)) {
      wakeupHandler.handle(reason);
    }
  }

  private MaterializationDescriptor getDescriptor(Materialization materialization) throws CacheException {
    final ReflectionGoal goal = userStore.get(materialization.getReflectionId());
    if (!goal.getVersion().equals(materialization.getReflectionGoalVersion())) {
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
                logger.debug("couldn't expand materialization {}", m.getId().getId(), e);
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
  }

  private final class CacheHelperImpl implements CacheHelper {
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
      final DremioRelOptMaterialization expanded = expand(descriptor);
      if (expanded == null) {
        return null;
      }
      return new CachedMaterializationDescriptor(descriptor, expanded);
    }

    @Override
    public DremioRelOptMaterialization expand(MaterializationDescriptor descriptor) {
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

        logger.debug("failed to expand materialization descriptor {}/{}. Associated reflection will be scheduled for update",
          descriptor.getLayoutId(), descriptor.getMaterializationId(), e);
        reflectionsToUpdate.add(new ReflectionId(descriptor.getLayoutId()));
        wakeupManager("failed to expand materialization"); // we should wake up the manager to update the reflection
        return null;
      } catch (MaterializationExpander.ExpansionException e) {
        logger.debug("failed to expand materialization descriptor {}/{}. Associated reflection will be scheduled for update",
          descriptor.getLayoutId(), descriptor.getMaterializationId(), e);
        reflectionsToUpdate.add(new ReflectionId(descriptor.getLayoutId()));
        wakeupManager("failed to expand materialization"); // we should wake up the manager to update the reflection
        return null;
      }
    }
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
        final long cacheUpdateDelay = getOptionManager().getOption(MATERIALIZATION_CACHE_REFRESH_DELAY_MILLIS);
        schedulerService.get().schedule(scheduleForRunningOnceAt(ofEpochMilli(System.currentTimeMillis() + cacheUpdateDelay)), this);
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
        context.getSubstitutionProviderFactory());
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
