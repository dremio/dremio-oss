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
import static com.dremio.service.reflection.DatasetHashUtils.computeDatasetHash;
import static com.dremio.service.reflection.ReflectionOptions.MATERIALIZATION_CACHE_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.MAX_NUM_REFLECTIONS_LIMIT;
import static com.dremio.service.reflection.ReflectionOptions.MAX_NUM_REFLECTIONS_LIMIT_ENABLED;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_ENABLE_SUBSTITUTION;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_MANAGER_REFRESH_DELAY_MILLIS;
import static com.dremio.service.reflection.ReflectionOptions.REFLECTION_PERIODIC_WAKEUP_ONLY;
import static com.dremio.service.reflection.ReflectionOptions.SUGGEST_REFLECTION_BASED_ON_TYPE;
import static com.dremio.service.reflection.ReflectionUtils.hasMissingPartitions;
import static com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType.ALL;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static com.google.common.base.Predicates.notNull;
import static java.time.Instant.ofEpochMilli;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.DremioVersionInfo;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.context.RequestContext;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.QueryContextCreator;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.MaterializationExpander;
import com.dremio.exec.planner.acceleration.descriptor.ExpandedMaterializationDescriptor;
import com.dremio.exec.planner.acceleration.descriptor.MaterializationDescriptor;
import com.dremio.exec.planner.acceleration.descriptor.UnexpandedMaterializationDescriptor;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.plancache.CacheRefresher;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.exec.store.sys.accel.AccelerationListManager.ReflectionLineageInfo;
import com.dremio.exec.store.sys.accel.AccelerationManager.ExcludedReflectionsProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.job.DeleteJobCountsRequest;
import com.dremio.service.job.UsedReflections;
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
import com.dremio.service.reflection.analysis.ReflectionSuggester.ReflectionSuggestionType;
import com.dremio.service.reflection.proto.ExternalReflection;
import com.dremio.service.reflection.proto.Failure;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationMetrics;
import com.dremio.service.reflection.proto.MaterializationPlan;
import com.dremio.service.reflection.proto.MaterializationPlanId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionEntry;
import com.dremio.service.reflection.proto.ReflectionField;
import com.dremio.service.reflection.proto.ReflectionGoal;
import com.dremio.service.reflection.proto.ReflectionGoalState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.ReflectionState;
import com.dremio.service.reflection.proto.ReflectionType;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.refresh.ReflectionPlanGenerator;
import com.dremio.service.reflection.refresh.RefreshHelper;
import com.dremio.service.reflection.store.DependenciesStore;
import com.dremio.service.reflection.store.ExternalReflectionStore;
import com.dremio.service.reflection.store.MaterializationPlanStore;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.service.reflection.store.ReflectionEntriesStore;
import com.dremio.service.reflection.store.ReflectionGoalsStore;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReflectionServiceImpl provides reflections to the planner in the form of
 * MaterializationDescriptors. A descriptor is only valid if the materialization is not expired and
 * has a logical plan associated to it that the planner can use for matching. For performance
 * reasons, ReflectionServiceImpl will maintain an internal MaterializationCache where the logical
 * plans are already de-serialized and expanded for immediate planner use.
 *
 * <p>ReflectionServiceImpl runs on every coordinator and maintains its own copy of the
 * MaterializationCache. If the coordinator is also master or master-less task leader, then
 * ReflectionServiceImpl will additionally run {@link ReflectionManager}.
 */
public class ReflectionServiceImpl extends BaseReflectionService {

  private static final Logger logger = LoggerFactory.getLogger(ReflectionServiceImpl.class);

  public static final String LOCAL_TASK_LEADER_NAME = "reflectionsrefresh";

  public static final String ACCELERATOR_STORAGEPLUGIN_NAME = "__accelerator";
  private static final String REFLECTION_MANAGER_FACTORY =
      "dremio.reflection.reflection-manager-factory.class";

  private static final Comparator<MaterializationDescriptor> JOB_START_COMPARATOR =
      Comparator.comparingLong(MaterializationDescriptor::getJobStart);

  @VisibleForTesting
  public static final MaterializationDescriptorFactory DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY =
      ReflectionUtils::getMaterializationDescriptor;

  /**
   * DescriptorCache enables {@link ReflectionManager} to make immediate updates to the {@link
   * MaterializationCache} on the master coordinator. Other coordinators don't run {@link
   * ReflectionManager} and the {@link MaterializationCache} on those coordinators are updated by
   * {@link CacheRefresher}
   */
  interface DescriptorCache {

    void invalidate(Materialization m);

    void update(Materialization m) throws CacheException, InterruptedException;
  }

  private MaterializationDescriptorProvider materializationDescriptorProvider;
  private final Provider<SchedulerService> schedulerService;
  private final Provider<JobsService> jobsService;
  private final Provider<NamespaceService> namespaceService;
  private final Provider<CatalogService> catalogService;
  private final Provider<SabotContext> sabotContext;
  private final Provider<ReflectionStatusService> reflectionStatusService;
  private final Supplier<ReflectionSettings> reflectionSettingsSupplier;
  private final ExecutorService executorService;
  private final BufferAllocator allocator;
  private final ReflectionSettings reflectionSettings;
  private final DatasetEventHub datasetEventHub;
  private final ReflectionGoalsStore userStore;
  private final ReflectionEntriesStore internalStore;
  private final MaterializationStore materializationStore;
  private final MaterializationPlanStore materializationPlanStore;
  private final ExternalReflectionStore externalReflectionStore;
  private final DependenciesStore dependenciesStore;
  private final RefreshRequestsStore requestsStore;
  private final boolean isMaster;
  private final CacheHelperImpl cacheHelper = new CacheHelperImpl();

  private MaterializationCache materializationCache;
  private ReflectionManagerWakeupHandler wakeupHandler;
  private boolean isMasterLessEnabled;

  private final CacheViewer cacheViewer =
      new CacheViewer() {
        @Override
        public boolean isCached(MaterializationId id) {
          return !isCacheEnabled() || materializationCache.contains(id);
        }

        @Override
        public boolean isInitialized() {
          return materializationCache.isInitialized();
        }
      };

  private final Function<Catalog, ExpansionHelper> expansionHelper;

  private final ReflectionValidator validator;

  private final MaterializationDescriptorFactory materializationDescriptorFactory;

  private final Provider<RequestContext> requestContextProvider;

  private ReflectionManager reflectionManager = null;

  private final Supplier<ReflectionManagerFactory> reflectionManagerFactorySupplier;

  public ReflectionServiceImpl(
      final SabotConfig config,
      final Provider<LegacyKVStoreProvider> storeProvider,
      final Provider<SchedulerService> schedulerService,
      final Provider<JobsService> jobsService,
      final Provider<CatalogService> catalogService,
      final Provider<SabotContext> sabotContext,
      final Provider<ReflectionStatusService> reflectionStatusService,
      final ExecutorService executorService,
      final boolean isMaster,
      final BufferAllocator allocator,
      final Provider<RequestContext> requestContextProvider,
      final DatasetEventHub datasetEventHub) {
    this.schedulerService =
        Preconditions.checkNotNull(schedulerService, "scheduler service required");
    this.jobsService = Preconditions.checkNotNull(jobsService, "jobs service required");
    this.catalogService = Preconditions.checkNotNull(catalogService, "catalog service required");
    this.sabotContext = Preconditions.checkNotNull(sabotContext, "acceleration plugin required");
    this.reflectionStatusService =
        Preconditions.checkNotNull(reflectionStatusService, "reflection status service required");
    this.executorService = Preconditions.checkNotNull(executorService, "executor service required");
    this.namespaceService = () -> sabotContext.get().getNamespaceService(SYSTEM_USERNAME);
    this.reflectionSettings =
        new ReflectionSettingsImpl(
            namespaceService, catalogService, storeProvider, this::getOptionManager);
    this.isMaster = isMaster;
    this.allocator = allocator.newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    this.requestContextProvider = requestContextProvider;

    userStore = new ReflectionGoalsStore(storeProvider);
    internalStore = new ReflectionEntriesStore(storeProvider);
    materializationStore = new MaterializationStore(storeProvider);
    materializationPlanStore = new MaterializationPlanStore(storeProvider);
    externalReflectionStore = new ExternalReflectionStore(storeProvider);
    dependenciesStore = new DependenciesStore(storeProvider);
    requestsStore = new RefreshRequestsStore(storeProvider);

    // The input catalog is expected to be shared across expansion helpers so that different
    // materializations can share the catalog metadata during expansion and plan rebuild.
    this.expansionHelper =
        catalog -> {
          SabotContext sabotCtx = sabotContext.get();
          QueryContextCreator queryCtxCreator = sabotCtx.getQueryContextCreator();
          UserSession session = UserSessionUtils.systemSession(sabotCtx.getOptionManager());
          // Explicitly disable view updates since updating a view could depend on reflections
          session
              .getSessionOptionManager()
              .setOption(
                  OptionValue.createBoolean(
                      OptionType.SESSION, PlannerSettings.VDS_AUTO_FIX.getOptionName(), false));
          QueryContext queryCtx =
              queryCtxCreator.createNewQueryContext(
                  session,
                  new AttemptId().toQueryId(),
                  null,
                  Long.MAX_VALUE,
                  Predicates.alwaysTrue(),
                  null,
                  null,
                  catalog);
          return new ExpansionHelper(queryCtx);
        };

    this.validator = new ReflectionValidator(catalogService, this::getOptionManager);
    this.materializationDescriptorFactory =
        config.getInstance(
            "dremio.reflection.materialization.descriptor.factory",
            MaterializationDescriptorFactory.class,
            DEFAULT_MATERIALIZATION_DESCRIPTOR_FACTORY);

    this.reflectionManagerFactorySupplier =
        Suppliers.memoize(
            () -> {
              final ReflectionManagerFactory defaultReflectionManagerFactory =
                  new ReflectionManagerFactory(
                      sabotContext,
                      storeProvider,
                      jobsService,
                      catalogService,
                      namespaceService,
                      executorService,
                      userStore,
                      internalStore,
                      externalReflectionStore,
                      materializationStore,
                      materializationPlanStore,
                      this::wakeupManager,
                      expansionHelper,
                      datasetEventHub,
                      requestsStore,
                      dependenciesStore,
                      allocator,
                      DescriptorCacheImpl::new);
              return config.getInstance(
                  ReflectionManagerFactory.REFLECTION_MANAGER_FACTORY,
                  ReflectionManagerFactory.class,
                  defaultReflectionManagerFactory,
                  defaultReflectionManagerFactory);
            });

    this.reflectionSettingsSupplier =
        Suppliers.memoize(() -> reflectionManagerFactorySupplier.get().newReflectionSettings());
    this.datasetEventHub = datasetEventHub;
  }

  @VisibleForTesting
  Function<Catalog, ExpansionHelper> getExpansionHelper() {
    return this.expansionHelper;
  }

  public MaterializationDescriptorProvider getMaterializationDescriptor() {
    return materializationDescriptorProvider;
  }

  @Override
  public void start() {
    logger.info("Reflection Service Starting");
    this.materializationDescriptorProvider = new MaterializationDescriptorProviderImpl();
    this.isMasterLessEnabled = sabotContext.get().getDremioConfig().isMasterlessEnabled();

    this.materializationCache = createMaterializationCache();

    final SchedulerService scheduler = schedulerService.get();
    // only start the managers on the master node
    if (isMaster) {
      if (!isMasterLessEnabled) {
        // if it is masterful mode just init
        taskLeaderInit();
      }
      // sends a wakeup event every reflection_manager_refresh_delay
      // does not relinquish
      // important to use schedule once and reschedule since option value might
      // change dynamically
      scheduler.schedule(
          Schedule.Builder.singleShotChain()
              .startingAt(ofEpochMilli(System.currentTimeMillis() + getRefreshTimeInMillis()))
              .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
              .sticky()
              .withCleanup(this::doCleanup)
              .build(),
          new Runnable() {
            @Override
            public void run() {
              if (scheduler.isRollingUpgradeInProgress(LOCAL_TASK_LEADER_NAME)) {
                logger.info(
                    "Reflection Service - Postponing taskLeaderInit as rolling upgrade is in progress");
              } else {
                logger.debug("Periodic refresh");
                if (wakeupHandler == null) {
                  taskLeaderInit();
                }
                wakeupManager("periodic refresh", true);
              }
              schedulerService
                  .get()
                  .schedule(
                      Schedule.Builder.singleShotChain()
                          .startingAt(
                              ofEpochMilli(System.currentTimeMillis() + getRefreshTimeInMillis()))
                          .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
                          .sticky()
                          .withCleanup(() -> doCleanup())
                          .build(),
                      this);
            }
          });
    }
  }

  @VisibleForTesting
  MaterializationCache createMaterializationCache() {
    return new MaterializationCache(
        cacheHelper,
        reflectionStatusService.get(),
        catalogService.get(),
        getOptionManager(),
        materializationStore);
  }

  /** Cleanup when we lose task leader status. May not be called if node is terminated. */
  private void doCleanup() {
    logger.info("Reflection Service - taskLeaderCleanup - lost ownership of RM schedule");
    reflectionManager = null;
    wakeupHandler = null;
  }

  private synchronized void taskLeaderInit() {
    if (wakeupHandler != null) {
      logger.info("Reflection Service - taskLeaderInit already done");
      return;
    }
    logger.info("Reflection Service - taskLeaderInit");

    final FileSystemPlugin accelerationPlugin =
        sabotContext
            .get()
            .getCatalogService()
            .getSource(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME);

    reflectionManager =
        reflectionManagerFactorySupplier
            .get()
            .newReflectionManager(reflectionSettingsSupplier.get(), requestContextProvider);

    wakeupHandler =
        reflectionManagerFactorySupplier
            .get()
            .newWakeupHandler(executorService, reflectionManager, requestContextProvider);
  }

  public RefreshHelper getRefreshHelper() {

    return new RefreshHelper() {

      @Override
      public ReflectionSettings getReflectionSettings() {
        return reflectionSettingsSupplier.get();
      }

      @Override
      public MaterializationStore getMaterializationStore() {
        return materializationStore;
      }

      @Override
      public CatalogService getCatalogService() {
        return catalogService.get();
      }

      @Override
      public DependenciesStore getDependenciesStore() {
        return dependenciesStore;
      }
    };
  }

  @Override
  public ExcludedReflectionsProvider getExcludedReflectionsProvider() {
    // Ideally there should be no requirement that the REFRESH REFLECTION job has to be planned
    // on the same coordinator as the task leader
    DependencyManager dependencyManager =
        Optional.ofNullable(reflectionManager)
            .map(ReflectionManager::getDependencyManager)
            .orElse(null);
    Preconditions.checkNotNull(
        dependencyManager, "REFLECTION REFRESH job not running on task leader");
    return dependencyManager.getExcludedReflectionsProvider();
  }

  private long getRefreshTimeInMillis() {
    return getOptionManager().getOption(REFLECTION_MANAGER_REFRESH_DELAY_MILLIS);
  }

  @VisibleForTesting
  public void refreshCache() {
    if (isCacheEnabled()) {
      logger.debug("Materialization cache refresh...");
      materializationCache.refreshMaterializationCache();
    } else {
      logger.debug("Materialization cache is disabled. Resetting materialization cache");
      materializationCache.resetCache();
    }
  }

  private boolean isCacheEnabled() {
    return getOptionManager().getOption(MATERIALIZATION_CACHE_ENABLED);
  }

  /**
   * @return non expired DONE materializations that have at least one refresh
   */
  @VisibleForTesting
  Iterable<Materialization> getValidMaterializations() {
    final long now = System.currentTimeMillis();
    return Iterables.filter(
        materializationStore.getAllDoneWhen(now),
        new Predicate<Materialization>() {
          @Override
          public boolean apply(Materialization m) {
            ReflectionEntry entry = internalStore.get(m.getReflectionId());
            return entry != null
                && entry.getState() != ReflectionState.FAILED
                && !Iterables.isEmpty(materializationStore.getRefreshes(m));
          }
        });
  }

  private Set<String> getActiveHosts() {
    return Sets.newHashSet(
        Iterables.transform(
            sabotContext.get().getClusterCoordinator().getExecutorEndpoints(),
            endpoint -> endpoint.getAddress()));
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  @Override
  public ReflectionId create(ReflectionGoal goal) {
    long maxAllowed = getOptionManager().getOption(MAX_NUM_REFLECTIONS_LIMIT);
    if (getOptionManager().getOption(MAX_NUM_REFLECTIONS_LIMIT_ENABLED)
        && userStore.getNumAllNotDeleted() >= maxAllowed) {
      throw UserException.validationError()
          .message("Maximum number of allowed reflections exceeded (%d)", maxAllowed)
          .buildSilently();
    }

    try {
      Preconditions.checkArgument(goal.getId() == null, "new reflection shouldn't have an ID");
      Preconditions.checkState(goal.getTag() == null, "new reflection shouldn't have a version");
      validator.validate(goal);
    } catch (Exception e) {
      throw UserException.validationError()
          .message("Invalid reflection: %s", e.getMessage())
          .build(logger);
    }

    final ReflectionId reflectionId = new ReflectionId(UUID.randomUUID().toString());
    goal.setId(reflectionId);

    userStore.save(goal);

    logger.debug("Created reflection goal for {}", ReflectionUtils.getId(goal));
    wakeupManager("reflection goal created");
    return reflectionId;
  }

  @Override
  public ReflectionId createExternalReflection(
      String name, List<String> datasetPath, List<String> targetDatasetPath) {
    ReflectionId id = new ReflectionId(UUID.randomUUID().toString());
    try {
      Catalog catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
      DatasetConfig datasetConfig =
          CatalogUtil.getDatasetConfig(catalog, new NamespaceKey(datasetPath));
      if (datasetConfig == null) {
        throw UserException.validationError()
            .message(String.format("Dataset %s not found", quotedCompound(datasetPath)))
            .build(logger);
      }

      DatasetConfig targetDatasetConfig =
          CatalogUtil.getDatasetConfig(catalog, new NamespaceKey(targetDatasetPath));
      if (targetDatasetConfig == null) {
        throw UserException.validationError()
            .message(String.format("Dataset %s not found", quotedCompound(targetDatasetPath)))
            .build(logger);
      }
      ExternalReflection externalReflection =
          new ExternalReflection()
              .setId(id.getId())
              .setName(name)
              .setQueryDatasetId(datasetConfig.getId().getId())
              .setQueryDatasetHash(computeDatasetHash(datasetConfig, catalog, true))
              .setTargetDatasetId(targetDatasetConfig.getId().getId())
              .setTargetDatasetHash(computeDatasetHash(targetDatasetConfig, catalog, true));

      // check that we are able to get a MaterializationDescriptor before storing it
      MaterializationDescriptor descriptor =
          ReflectionUtils.getMaterializationDescriptor(externalReflection, catalog);
      if (descriptor == null) {
        throw UserException.validationError()
            .message("Failed to validate external reflection " + name)
            .build(logger);
      }

      // validate that we can convert to a materialization
      try (ExpansionHelper helper = getExpansionHelper().apply(catalog)) {
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
    return Optional.ofNullable(externalReflectionStore.get(id));
  }

  @Override
  public Iterable<ExternalReflection> getExternalReflectionByDatasetPath(List<String> datasetPath) {
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    DatasetConfig datasetConfig =
        CatalogUtil.getDatasetConfig(catalog, new NamespaceKey(datasetPath));
    if (datasetConfig == null) {
      throw UserException.validationError()
          .message(String.format("Dataset %s not found", quotedCompound(datasetPath)))
          .build(logger);
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
    try {
      jobsService
          .get()
          .deleteJobCounts(
              DeleteJobCountsRequest.newBuilder()
                  .setReflections(UsedReflections.newBuilder().addReflectionIds(id).build())
                  .build());
    } catch (Exception e) {
      logger.error("Unable to delete job counts for external reflection : {}", id, e);
    }
  }

  @Override
  public void update(ReflectionGoal goal) {
    try {
      Preconditions.checkNotNull(goal, "reflection goal required");
      Preconditions.checkNotNull(goal.getId(), "reflection id required");
      Preconditions.checkNotNull(goal.getTag(), "reflection version required");

      ReflectionGoal currentGoal =
          getGoal(goal.getId())
              .orElseThrow(
                  () ->
                      UserException.validationError()
                          .message("Reflection not found: %s", goal.getId())
                          .build(logger));

      if (currentGoal.getState() == ReflectionGoalState.DELETED) {
        throw UserException.validationError()
            .message("Cannot update a deleted reflection")
            .build(logger);
      }

      if (currentGoal.getType() != goal.getType()) {
        throw UserException.validationError()
            .message("Cannot change the type of an existing reflection")
            .build(logger);
      }

      if (!currentGoal.getDatasetId().equals(goal.getDatasetId())) {
        throw UserException.validationError()
            .message("Cannot change the dataset id of an existing reflection")
            .build(logger);
      }

      if (goal.getState() == ReflectionGoalState.ENABLED) {
        validator.validate(goal);
      }

      // check if anything has changed - if not, don't bother updating
      if (currentGoal.getName().equals(goal.getName())
          && currentGoal.getArrowCachingEnabled().equals(goal.getArrowCachingEnabled())
          && currentGoal.getState() == goal.getState()
          && ReflectionUtils.areReflectionDetailsEqual(
              currentGoal.getDetails(), goal.getDetails())) {
        return;
      }

      // Inherit the createdAt time from the current goal.
      goal.setCreatedAt(currentGoal.getCreatedAt());
    } catch (Exception e) {
      throw UserException.validationError()
          .message("Invalid reflection: %s", e.getMessage())
          .build(logger);
    }

    userStore.save(goal);
    wakeupManager("reflection goal updated");
  }

  @Override
  public Optional<ReflectionEntry> getEntry(ReflectionId reflectionId) {
    return Optional.ofNullable(internalStore.get(reflectionId));
  }

  @Override
  public void saveEntry(ReflectionEntry reflectionEntry) {
    int retryCount = 0;
    ConcurrentModificationException cme = null;
    while (retryCount < 3) {
      try {
        internalStore.save(reflectionEntry);
        break;
      } catch (ConcurrentModificationException e) {
        retryCount++;
        cme = e;
      }
    }
    if (retryCount >= 3) {
      logger.error(
          "Error while trying to save reflection entry for reflection {} ",
          reflectionEntry.getId().getId(),
          cme);
    }
  }

  @Override
  public Optional<ReflectionGoal> getGoal(ReflectionId reflectionId) {
    final ReflectionGoal goal = userStore.get(reflectionId);
    if (goal == null || goal.getState() == ReflectionGoalState.DELETED) {
      return Optional.empty();
    }
    return Optional.of(goal);
  }

  @Override
  public void clearAll() {
    final Iterable<ReflectionGoal> reflections = userStore.getAll();
    List<String> deleteReflectionIds = new ArrayList<>();
    for (ReflectionGoal goal : reflections) {
      if (goal.getState() != ReflectionGoalState.DELETED) {
        userStore.save(goal.setState(ReflectionGoalState.DELETED));
        deleteReflectionIds.add(goal.getId().getId());
      }
    }
    try {
      if (deleteReflectionIds.size() > 0) {
        jobsService
            .get()
            .deleteJobCounts(
                DeleteJobCountsRequest.newBuilder()
                    .setReflections(
                        UsedReflections.newBuilder()
                            .addAllReflectionIds(deleteReflectionIds)
                            .build())
                    .build());
      }
    } catch (Exception e) {
      logger.error("Unable to delete job counts for reflection : {}", deleteReflectionIds, e);
    }
  }

  @Override
  public void retryUnavailable() {
    for (ReflectionGoal goal : userStore.getAll()) {
      if (goal.getType() == ReflectionType.EXTERNAL
          || goal.getState() != ReflectionGoalState.ENABLED) {
        continue;
      }
      try {
        if (materializationStore.getLastMaterializationDone(goal.getId()) == null) {
          userStore.save(goal);
        }
      } catch (RuntimeException e) {
        logger.warn("Unable to retry unavailable {}", ReflectionUtils.getId(goal), e);
      }
    }
  }

  @Override
  public void clean() {
    for (Map.Entry<MaterializationPlanId, MaterializationPlan> plan :
        materializationPlanStore.getAll()) {
      try {
        materializationPlanStore.delete(plan.getKey());
      } catch (RuntimeException e) {
        logger.warn("Unable to clean {}", plan.getValue().getId().toString());
      }
    }
  }

  @VisibleForTesting
  public Iterable<DependencyEntry> getDependencies(ReflectionId reflectionId) {
    // Only used for testing so that we can validate reflection dependencies
    return reflectionManager.getDependencyManager().getDependencies(reflectionId);
  }

  private Stream<AccelerationListManager.DependencyInfo> getGoalDependencies(ReflectionGoal goal) {
    ReflectionId goalId = goal.getId();
    final List<DependencyEntry> dependencyEntries =
        reflectionManager.getDependencyManager().getDependencies(goalId);

    return StreamSupport.stream(dependencyEntries.spliterator(), false)
        .map(
            new Function<DependencyEntry, AccelerationListManager.DependencyInfo>() {
              @Override
              public AccelerationListManager.DependencyInfo apply(DependencyEntry entry) {
                return new AccelerationListManager.DependencyInfo(
                    goalId.getId(),
                    entry.getId(),
                    entry.getType().toString(),
                    entry.getPath().toString());
              }
            });
  }

  @Override
  public Iterator<AccelerationListManager.DependencyInfo> getReflectionDependencies() {
    final Iterable<ReflectionGoal> goalReflections = getAllReflections();
    return StreamSupport.stream(goalReflections.spliterator(), false)
        .flatMap(this::getGoalDependencies)
        .iterator();
  }

  @Override
  public Iterable<ReflectionGoal> getAllReflections() {
    return ReflectionUtils.getAllReflections(userStore);
  }

  @VisibleForTesting
  @Override
  public Iterable<ReflectionGoal> getReflectionsByDatasetPath(CatalogEntityKey path) {
    String datasetId = null;
    final Catalog catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    // TODO: DX-88987
    // Use Catalog.getDatasetId with full CatalogEntityKey including version context. This class
    // shouldn't have to decide how to act according to the source type.
    if (CatalogUtil.requestedPluginSupportsVersionedTables(path.getRootEntity(), catalog)) {
      DremioTable table = catalog.getTable(path);
      if (table != null) {
        datasetId = table.getDatasetConfig().getId().getId();
      }
    } else {
      datasetId = catalog.getDatasetId(path.toNamespaceKey());
    }

    if (datasetId == null) {
      return Collections.emptyList();
    }
    return getReflectionsByDatasetId(datasetId);
  }

  @VisibleForTesting
  public Iterable<ReflectionGoal> getReflectionGoals(
      final NamespaceKey path, final String reflectionName) {
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, path);
    if (datasetConfig == null) {
      Throwables.propagate(new NamespaceNotFoundException(path, "Dataset not found in catalog"));
    }
    return FluentIterable.from(getReflectionsByDatasetId(datasetConfig.getId().getId()))
        .filter(
            new Predicate<ReflectionGoal>() {

              @Override
              public boolean apply(ReflectionGoal input) {
                return reflectionName.equals(input.getName());
              }
            });
  }

  @Override
  @WithSpan
  public Iterable<ReflectionGoal> getReflectionsByDatasetId(String datasetId) {
    return userStore.getByDatasetId(datasetId);
  }

  @Override
  public int getEnabledReflectionCountForDataset(String datasetId) {
    return userStore.getEnabledByDatasetId(datasetId);
  }

  @Override
  public Optional<Materialization> getLastDoneMaterialization(ReflectionId reflectionId) {
    final Materialization materialization =
        materializationStore.getLastMaterializationDone(reflectionId);
    if (materialization == null) {
      return Optional.empty();
    }
    return Optional.of(materialization);
  }

  @Override
  public void setSubstitutionEnabled(boolean enable) {
    getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                SYSTEM, REFLECTION_ENABLE_SUBSTITUTION.getOptionName(), enable));
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
    if (goal.isPresent() && goal.get().getState() != ReflectionGoalState.DELETED) {
      update(goal.get().setState(ReflectionGoalState.DELETED));
      try {
        jobsService
            .get()
            .deleteJobCounts(
                DeleteJobCountsRequest.newBuilder()
                    .setReflections(
                        UsedReflections.newBuilder()
                            .addReflectionIds(goal.get().getId().getId())
                            .build())
                    .build());
      } catch (Exception e) {
        logger.error(
            "Unable to delete job counts for reflection : {}", goal.get().getId().getId(), e);
      }

      datasetEventHub.fireRemoveDatasetEvent(new DatasetRemovedEvent(goal.get().getDatasetId()));
    }
  }

  @Override
  public void remove(ReflectionGoal goal) {
    update(goal.setState(ReflectionGoalState.DELETED));
    try {
      jobsService
          .get()
          .deleteJobCounts(
              DeleteJobCountsRequest.newBuilder()
                  .setReflections(
                      UsedReflections.newBuilder().addReflectionIds(goal.getId().getId()).build())
                  .build());
    } catch (Exception e) {
      logger.error("Unable to delete job counts for reflection : {}", goal.getId().getId(), e);
    }

    datasetEventHub.fireRemoveDatasetEvent(new DatasetRemovedEvent(goal.getDatasetId()));
  }

  @Override
  public Optional<Materialization> getMaterialization(MaterializationId materializationId) {
    return Optional.ofNullable(materializationStore.get(materializationId));
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
  @WithSpan
  public List<ReflectionGoal> getRecommendedReflections(
      String datasetId, ReflectionSuggestionType type) {
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    DatasetConfig datasetConfig = CatalogUtil.getDatasetConfig(catalog, datasetId);

    if (datasetConfig == null) {
      throw new NotFoundException("Dataset not found");
    }

    ReflectionAnalyzer analyzer =
        new ReflectionAnalyzer(jobsService.get(), catalogService.get(), allocator);
    ReflectionSuggester suggester = new ReflectionSuggester(datasetConfig);

    if (getOptionManager().getOption(SUGGEST_REFLECTION_BASED_ON_TYPE)) {
      TableStats tableStats = analyzer.analyzeForType(datasetId, type);
      return suggester.getReflectionGoals(tableStats, type);
    } else {
      TableStats tableStats = analyzer.analyze(datasetId);
      return suggester.getReflectionGoals(tableStats, ALL);
    }
  }

  @Override
  public ReflectionSettings getReflectionSettings() {
    return reflectionSettingsSupplier.get();
  }

  @Override
  public void requestRefresh(String datasetId) {
    logger.debug("Refresh requested on dataset {}", datasetId);
    RefreshRequest request = requestsStore.get(datasetId);
    if (request == null) {
      request = new RefreshRequest().setDatasetId(datasetId).setRequestedAt(0L);
    }
    request.setRequestedAt(Math.max(System.currentTimeMillis(), request.getRequestedAt()));
    requestsStore.save(datasetId, request);
    wakeupManager("refresh request for dataset " + datasetId);
  }

  @Override
  public Future<?> wakeupManager(String reason) {
    return wakeupManager(reason, false);
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

  public ReflectionGoalsStore getRelfectionGoalsStore() {
    return userStore;
  }

  public ReflectionEntriesStore getReflectionEntriesStore() {
    return internalStore;
  }

  private Future<?> wakeupManager(String reason, boolean periodic) {
    // in master-less mode, periodic wake-ups occur only on task leader. Ad-hoc wake-ups can occur
    // on any coordinator,
    // and can cause result in concurrent modification exceptions, when updating kvstore - so,
    // disable them.
    final boolean periodicWakeupOnly =
        getOptionManager().getOption(REFLECTION_PERIODIC_WAKEUP_ONLY) || isMasterLessEnabled;
    if (wakeupHandler != null && (!periodicWakeupOnly || periodic)) {
      return wakeupHandler.handle(reason);
    }
    return CompletableFuture.completedFuture(null);
  }

  @VisibleForTesting
  UnexpandedMaterializationDescriptor getDescriptor(
      Materialization materialization, Catalog catalog, ReflectionPlanGeneratorProvider provider)
      throws CacheException {
    final ReflectionGoal goal = userStore.get(materialization.getReflectionId());
    if (!ReflectionGoalChecker.checkGoal(goal, materialization)) {
      Materialization update = materializationStore.get(materialization.getId());
      update.setState(MaterializationState.FAILED);
      update.setFailure(
          new Failure()
              .setMessage(
                  "Reflection definition has changed and materialization is no longer valid."));
      try {
        materializationStore.save(update);
      } catch (ConcurrentModificationException e2) {
        // ignore in case another coordinator also tries to mark the materialization as failed
      }
      // reflection goal changed and corresponding materialization is no longer valid
      throw new CacheException(
          "Unable to expand materialization "
              + materialization.getId().getId()
              + " as it no longer matches its reflection goal");
    }

    final ReflectionEntry entry = internalStore.get(materialization.getReflectionId());

    MaterializationMetrics metrics = materializationStore.getMetrics(materialization).left;

    MaterializationPlan plan =
        this.materializationPlanStore.getVersionedPlan(materialization.getId());
    if (plan == null) {
      try (ExpansionHelper helper = getExpansionHelper().apply(catalog)) {
        ReflectionPlanGenerator generator =
            provider.create(
                helper,
                catalogService.get(),
                sabotContext.get().getConfig(),
                goal,
                entry,
                materialization,
                reflectionSettingsSupplier.get(),
                materializationStore,
                dependenciesStore,
                false,
                true);
        generator.generateNormalizedPlan();
        ByteString logicalPlanBytes = generator.getMatchingPlanBytes();
        plan = new MaterializationPlan();
        plan.setId(MaterializationPlanStore.createMaterializationPlanId(materialization.getId()));
        plan.setMaterializationId(materialization.getId());
        plan.setReflectionId(materialization.getReflectionId());
        plan.setVersion(DremioVersionInfo.getVersion());
        plan.setLogicalPlan(logicalPlanBytes);
        materializationPlanStore.save(plan);
        logger.info(
            "Materialization plan upgrade: Successfully rebuilt plan for {}",
            ReflectionUtils.getId(goal));
      } catch (ConcurrentModificationException e) {
        logger.warn(
            "Materialization plan upgrade: Plan already rebuilt by another coordinator {}",
            ReflectionUtils.getId(goal));
      } catch (RuntimeException e) {
        final String failureMsg =
            String.format(
                "Materialization plan upgrade: Unable to rebuild plan for %s. %s",
                ReflectionUtils.getId(goal), e.getMessage());
        throw new MaterializationExpander.RebuildPlanException(failureMsg, e);
      }
    }
    return materializationDescriptorFactory.getMaterializationDescriptor(
        goal, entry, materialization, plan, metrics.getOriginalCost(), catalogService.get());
  }

  private final class MaterializationDescriptorProviderImpl
      implements MaterializationDescriptorProvider {

    @Override
    public List<MaterializationDescriptor> get() {

      if (!isSubstitutionEnabled()) {
        return Collections.emptyList();
      }

      final long currentTime = System.currentTimeMillis();
      final Set<String> activeHosts = getActiveHosts();
      FluentIterable<MaterializationDescriptor> descriptors;
      if (isCacheEnabled()) {
        descriptors =
            FluentIterable.from(materializationCache.getAll())
                .filter(
                    new Predicate<MaterializationDescriptor>() {
                      @Override
                      public boolean apply(MaterializationDescriptor descriptor) {
                        return descriptor.getExpirationTimestamp() > currentTime
                            && activeHosts.containsAll(descriptor.getPartition());
                      }
                    });
      } else {
        final Catalog catalog =
            CatalogUtil.getSystemCatalogForMaterializationCache(catalogService.get());
        descriptors =
            FluentIterable.from(getValidMaterializations())
                .filter(
                    new Predicate<Materialization>() {
                      @Override
                      public boolean apply(Materialization m) {
                        return !hasMissingPartitions(m.getPartitionList(), activeHosts);
                      }
                    })
                .transform(
                    new Function<Materialization, MaterializationDescriptor>() {
                      @Override
                      public MaterializationDescriptor apply(Materialization m) {
                        try {
                          // we don't need to expand here, but we do so to be able to update
                          // reflections when we fail to expand
                          // their materializations
                          return cacheHelper.expand(m, catalog);
                        } catch (Exception e) {
                          logger.warn("couldn't expand materialization {}", m.getId().getId(), e);
                          return null;
                        }
                      }
                    });

        Iterable<MaterializationDescriptor> externalDescriptors =
            FluentIterable.from(getAllExternalReflections())
                .transform(
                    new Function<ExternalReflection, MaterializationDescriptor>() {
                      @Nullable
                      @Override
                      public MaterializationDescriptor apply(
                          ExternalReflection externalReflection) {
                        try {
                          return ReflectionUtils.getMaterializationDescriptor(
                              externalReflection, catalog);
                        } catch (Exception e) {
                          logger.debug(
                              "failed to get MaterializationDescriptor for external reflection {}",
                              externalReflection.getName());
                          return null;
                        }
                      }
                    });

        descriptors = descriptors.append(externalDescriptors).filter(notNull());
      }

      if (Iterables.isEmpty(descriptors)) {
        return Collections.emptyList();
      }

      // group the materializations by reflectionId and keep one by reflection
      final ImmutableListMultimap<ReflectionId, MaterializationDescriptor> descriptorMap =
          descriptors.index(
              new Function<MaterializationDescriptor, ReflectionId>() {
                @Override
                public ReflectionId apply(MaterializationDescriptor m) {
                  return new ReflectionId(m.getLayoutId());
                }
              });
      // for each reflection, get latest materialization
      final Ordering<MaterializationDescriptor> ordering = Ordering.from(JOB_START_COMPARATOR);
      return FluentIterable.from(descriptorMap.keySet())
          .transform(
              new Function<ReflectionId, MaterializationDescriptor>() {
                @Override
                public MaterializationDescriptor apply(ReflectionId reflectionId) {
                  return ordering.max(descriptorMap.get(reflectionId));
                }
              })
          .toList();
    }

    /**
     * Checks if default raw reflection has been enabled/disabled on the VDS. Uses the same caching
     * catalog from planning so table metadata should already be in cache.
     */
    private boolean isDefaultReflectionEnabled(DremioTable viewTable) {
      DatasetConfig datasetConfig = viewTable.getDatasetConfig();
      if (!datasetConfig.getType().equals(DatasetType.VIRTUAL_DATASET)
          || datasetConfig.getVirtualDataset() == null) {
        return false;
      }
      return Optional.ofNullable(datasetConfig.getVirtualDataset().getDefaultReflectionEnabled())
          .orElse(true);
    }

    @Override
    public java.util.Optional<MaterializationDescriptor> getDefaultRawMaterialization(
        ViewTable viewTable) {

      if (isSubstitutionEnabled()) {
        try {
          final BatchSchema batchSchema = viewTable.getSchema();
          final List<String> vdsFields =
              batchSchema == null
                  ? new ArrayList<>()
                  : batchSchema.getFields().stream()
                      .map(Field::getName)
                      .sorted()
                      .collect(Collectors.toList());

          for (ReflectionGoal goal :
              getReflectionsByDatasetId(viewTable.getDatasetConfig().getId().getId())) {
            if (goal.getType() == ReflectionType.RAW) {
              List<String> displayFields =
                  goal.getDetails().getDisplayFieldList().stream()
                      .map(ReflectionField::getName)
                      .sorted()
                      .collect(Collectors.toList());
              if (displayFields.equals(vdsFields)) {
                final long currentTime = System.currentTimeMillis();
                final Set<String> activeHosts = getActiveHosts(); // Deprecated

                Materialization m = materializationStore.getLastMaterializationDone(goal.getId());
                if (m == null
                    || Iterables.isEmpty(materializationStore.getRefreshes(m))
                    || hasMissingPartitions(m.getPartitionList(), activeHosts)) {
                  continue;
                }
                ExpandedMaterializationDescriptor descriptor = null;
                // First try to get the descriptor from the materialization cache
                if (isCacheEnabled()) {
                  descriptor =
                      (ExpandedMaterializationDescriptor) materializationCache.get(m.getId());
                  // Check that materialization has not expired and all partitions are online
                  // (deprecated)
                  if (descriptor != null
                      && (descriptor.getExpirationTimestamp() < currentTime
                          || !activeHosts.containsAll(descriptor.getPartition()))) {
                    descriptor = null;
                  }
                }
                // If not found because cache is disabled or not online yet, then expand it on the
                // fly
                if (descriptor == null) {
                  try {
                    descriptor =
                        cacheHelper.expand(
                            m,
                            CatalogUtil.getSystemCatalogForMaterializationCache(
                                catalogService.get()));
                  } catch (Exception e) {
                    logger.warn("Couldn't expand materialization {}", m.getId().getId(), e);
                    continue; // There may be other DRRs for the same view that are available
                  }
                }
                if (descriptor != null) {
                  // We expect the feature to be enabled in the majority of cases. So we want to
                  // wait until
                  // we know that there are default reflections available for the dataset. This way
                  // we avoid
                  // hitting the namspace for every dataset in the tree, even if the dataset doesn't
                  // have any
                  // reflections
                  if (!isDefaultReflectionEnabled(viewTable)) {
                    return java.util.Optional.empty();
                  }
                  if (!(descriptor
                              .getMaterialization()
                              .getIncrementalUpdateSettings()
                              .isIncremental()
                          && descriptor.getMaterialization().hasAgg())
                      || getOptionManager()
                          .getOption(
                              ReflectionOptions
                                  .ENABLE_INCREMENTAL_DEFAULT_RAW_REFLECTIONS_WITH_AGGS)) {
                    // Do not apply default reflections for incremental refresh if there is an agg
                    // in the query plan
                    // unless we have the support key enabled.
                    return java.util.Optional.of(descriptor);
                  }
                }
              }
            }
          }
        } catch (Exception ex) {
          if (ex.getCause() instanceof NamespaceNotFoundException) {
            logger.debug(
                "Error while expanding view with path {}: {}",
                viewTable.getPath().getSchemaPath(),
                ex.getMessage());
          } else {
            Throwables.propagate(ex);
          }
        }
      }
      return java.util.Optional.empty();
    }

    @Override
    public boolean isMaterializationCacheInitialized() {
      return materializationCache.isInitialized();
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
    public MaterializationDescriptor getDescriptor(
        ExternalReflection externalReflection, EntityExplorer catalog) throws CacheException {
      try {
        return ReflectionUtils.getMaterializationDescriptor(externalReflection, catalog);
      } catch (NamespaceException e) {
        throw new CacheException("Unable to get descriptor for " + externalReflection.getName());
      }
    }

    @Override
    public ExpandedMaterializationDescriptor expand(
        Materialization materialization, Catalog catalog) throws CacheException {
      final MaterializationDescriptor descriptor =
          ReflectionServiceImpl.this.getDescriptor(
              materialization, catalog, ReflectionPlanGeneratorProvider.DEFAULT);
      final DremioMaterialization expanded = expand(descriptor, catalog);
      return new ExpandedMaterializationDescriptor(descriptor, expanded);
    }

    @Override
    public DremioMaterialization expand(MaterializationDescriptor descriptor, Catalog catalog) {
      // get a new converter for each materialization. This ensures that we
      // always index flattens from zero. This is a partial fix for flatten
      // matching. We should really do a better job in matching.
      try (ExpansionHelper helper = getExpansionHelper().apply(catalog)) {
        return descriptor.getMaterializationFor(helper.getConverter());
      }
    }
  }

  @Override
  public Optional<ReflectionManager> getReflectionManager() {
    return Optional.of(reflectionManager);
  }

  @Override
  public Iterator<ReflectionLineageInfo> getReflectionLineage(ReflectionGoal reflectionGoal) {
    EntityExplorer catalog = CatalogUtil.getSystemCatalogForReflections(catalogService.get());
    Map<ReflectionId, Integer> reflectionLineage =
        reflectionManager.computeReflectionLineage(reflectionGoal.getId());
    Stream<ReflectionLineageInfo> reflections =
        reflectionLineage.entrySet().stream()
            .map(
                v -> {
                  try {
                    ReflectionGoal goal = getGoal(v.getKey()).get();
                    String datasetName =
                        Optional.ofNullable(catalog.getTable(goal.getDatasetId()))
                            .map(DremioTable::getDatasetConfig)
                            .map(DatasetConfig::getName)
                            .orElse("Can't find dataset");
                    return new ReflectionLineageInfo(
                        v.getValue(), goal.getId().getId(), goal.getName(), datasetName);
                  } catch (Exception e) {
                    throw UserException.reflectionError(e)
                        .message(
                            String.format(
                                "Unable to get ReflectionInfo for %s", reflectionGoal.getId()))
                        .buildSilently();
                  }
                })
            .sorted(Comparator.comparingInt(ReflectionLineageInfo::getBatchNumber));
    return reflections.iterator();
  }

  private final class DescriptorCacheImpl implements DescriptorCache {

    @Override
    public void invalidate(Materialization m) {
      if (isCacheEnabled()) {
        logger.debug("Invalidating cache entry for {}", ReflectionUtils.getId(m));
        materializationCache.invalidate(m.getId());
      }
    }

    /**
     * This should be eventually deprecated as we want all materialization cache maintenance to be
     * done handled by the background thread and not one off when RM and materialization cache
     * happen to run on same coordinator.
     */
    @Override
    public void update(Materialization m) throws CacheException, InterruptedException {
      if (isCacheEnabled()) {
        logger.debug("Updating cache entry for {}", ReflectionUtils.getId(m));
        materializationCache.update(m);
      }
    }
  }

  @VisibleForTesting
  public interface ReflectionPlanGeneratorProvider {

    ReflectionPlanGenerator create(
        ExpansionHelper helper,
        CatalogService catalogService,
        SabotConfig sabotConfig,
        ReflectionGoal goal,
        ReflectionEntry entry,
        Materialization materialization,
        ReflectionSettings reflectionSettings,
        MaterializationStore materializationStore,
        DependenciesStore dependenciesStore,
        boolean forceFullUpdate,
        boolean isRebuildPlan);

    ReflectionPlanGeneratorProvider DEFAULT =
        (ExpansionHelper helper,
            CatalogService catalogService,
            SabotConfig sabotConfig,
            ReflectionGoal goal,
            ReflectionEntry entry,
            Materialization materialization,
            ReflectionSettings reflectionSettings,
            MaterializationStore materializationStore,
            DependenciesStore dependenciesStore,
            boolean forceFullUpdate,
            boolean isRebuildPlan) -> {
          SqlHandlerConfig sqlHandlerConfig =
              new SqlHandlerConfig(
                  helper.getContext(), helper.getConverter(), AttemptObservers.of(), null);
          return new ReflectionPlanGenerator(
              sqlHandlerConfig,
              catalogService,
              sabotConfig,
              goal,
              entry,
              materialization,
              reflectionSettings,
              materializationStore,
              dependenciesStore,
              forceFullUpdate,
              isRebuildPlan);
        };
  }

  /**
   * Materialization expansion helper that takes care of releasing the query context when closed.
   * Caller must close the helper when done using the converter
   */
  public static class ExpansionHelper implements AutoCloseable {

    private final QueryContext context;
    private final SqlConverter converter;

    public ExpansionHelper(QueryContext context) {
      this.context = Preconditions.checkNotNull(context, "query context required");
      converter =
          new SqlConverter(
              context.getPlannerSettings(),
              context.getOperatorTable(),
              context,
              MaterializationDescriptorProvider.EMPTY,
              context.getFunctionRegistry(),
              context.getSession(),
              AbstractAttemptObserver.NOOP,
              context.getSubstitutionProviderFactory(),
              context.getConfig(),
              context.getScanResult(),
              context.getRelMetadataQuerySupplier());
    }

    public SqlConverter getConverter() {
      return converter;
    }

    @Override
    public void close() {
      converter.dispose();
      AutoCloseables.closeNoChecked(context);
    }

    public QueryContext getContext() {
      return context;
    }
  }
}
