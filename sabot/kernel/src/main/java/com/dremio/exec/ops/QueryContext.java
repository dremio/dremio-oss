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
package com.dremio.exec.ops;

import static com.dremio.exec.ExecConstants.ENABLE_PARTITION_STATS_USAGE;
import static com.dremio.proto.model.PartitionStats.PartitionStatsValue;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.util.Arrays.asList;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.catalog.ImmutableMetadataRequestOptions;
import com.dremio.exec.catalog.MetadataRequestOptions;
import com.dremio.exec.catalog.MetadataStatsCollector;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalogImpl;
import com.dremio.exec.expr.ExpressionSplitCache;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.PlanCache;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.substitution.DefaultSubstitutionProviderFactory;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.planner.logical.partition.PartitionStatsBasedPrunerCache;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.normalizer.PlannerBaseComponent;
import com.dremio.exec.planner.normalizer.PlannerNormalizerComponent;
import com.dremio.exec.planner.normalizer.PlannerNormalizerComponentImpl;
import com.dremio.exec.planner.normalizer.PlannerNormalizerModule;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.sql.ViewExpander;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.WorkloadType;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotQueryContext;
import com.dremio.exec.server.SimpleJobRunner;
import com.dremio.exec.server.options.EagerCachingOptionManager;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.EndPointListProvider;
import com.dremio.exec.store.PartitionExplorer;
import com.dremio.exec.store.PartitionExplorerImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.util.Utilities;
import com.dremio.exec.work.WorkStats;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionResolver;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.partitionstats.cache.PartitionStatsCache;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.exec.context.CompilationOptions;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.exec.context.ContextInformationImpl;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts
// **NOTE** Ensure that an instantiated QueryContext is closed properly using a try-with-resources
// statement
// Or else dangling child allocators could cause issues when closing resources finally in the daemon
public class QueryContext
    implements AutoCloseable, ResourceSchedulingContext, OptimizerRulesContext {
  private final SabotQueryContext sabotQueryContext;
  private final UserSession session;
  private final QueryId queryId;

  private final OptionManager optionManager;
  private final QueryOptionManager queryOptionManager;
  private final ExecutionControls executionControls;
  private final PlannerSettings plannerSettings;
  private final SqlOperatorTable table;

  private final QueryContextInformation queryContextInfo;
  protected ContextInformation contextInformation;

  private final BufferAllocator allocator;
  private final BufferManager bufferManager;

  private final Catalog catalog;
  private final SubstitutionProviderFactory substitutionProviderFactory;
  private final FunctionImplementationRegistry functionImplementationRegistry;
  private GroupResourceInformation groupResourceInformation;

  /* Stores constants and their holders by type */
  private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;
  private final Map<List<String>, Map<String, Long>> survivingRowCountsWithPruneFilter;
  private final Map<List<String>, Map<String, Long>> survivingFileCountsWithPruneFilter;

  /* Stores error contexts registered with this function context **/
  private int nextErrorContextId = 0;
  private final List<FunctionErrorContext> errorContexts;
  protected final QueryPriority queryPriority;
  protected final Predicate<DatasetConfig> datasetValidityChecker;
  protected final WorkloadType workloadType;
  private final RelMetadataQuerySupplier relMetadataQuerySupplier;

  private final PlanCache planCache;
  private final PartitionStatsCache partitionStatsPredicateCache;

  /*
   * Flag to indicate if close has been called, after calling close the first
   * time this is set to true and the close method becomes a no-op.
   */
  private boolean closed = false;

  /*
   * Flag to indicate query requires groups info
   * (so that groups info needs to be available on executor).
   * E.g. When query contains unresolvable "is_member()" function.
   */
  private boolean queryRequiresGroupsInfo = false;

  public QueryContext(
      final UserSession session, final SabotQueryContext sabotQueryContext, QueryId queryId) {
    this(
        session,
        sabotQueryContext,
        queryId,
        null,
        Long.MAX_VALUE,
        Predicates.alwaysTrue(),
        null,
        null);
  }

  public QueryContext(
      final UserSession session,
      final SabotQueryContext sabotQueryContext,
      QueryId queryId,
      QueryPriority priority,
      long maxAllocation,
      Predicate<DatasetConfig> datasetValidityChecker,
      PlanCache planCache,
      PartitionStatsCache partitionStatsCache) {
    this.sabotQueryContext = sabotQueryContext;
    this.session = session;
    this.queryId = queryId;
    this.planCache = planCache;
    this.partitionStatsPredicateCache = partitionStatsCache;

    this.queryOptionManager = new QueryOptionManager(sabotQueryContext.getOptionValidatorListing());
    this.optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(
                new DefaultOptionManager(sabotQueryContext.getOptionValidatorListing()))
            .withOptionManager(
                new EagerCachingOptionManager(sabotQueryContext.getSystemOptionManager()))
            .withOptionManager(session.getSessionOptionManager())
            .withOptionManager(queryOptionManager)
            .build();
    this.executionControls = new ExecutionControls(optionManager, sabotQueryContext.getEndpoint());
    this.plannerSettings =
        new PlannerSettings(
            sabotQueryContext.getConfig(),
            optionManager,
            () -> groupResourceInformation,
            executionControls,
            getStatisticsService());
    functionImplementationRegistry =
        this.optionManager.getOption(PlannerSettings.ENABLE_DECIMAL_V2)
            ? sabotQueryContext.getDecimalFunctionImplementationRegistry()
            : sabotQueryContext.getFunctionImplementationRegistry();
    this.table = DremioCompositeSqlOperatorTable.create(functionImplementationRegistry);

    this.queryPriority = priority;
    this.workloadType = Utilities.getWorkloadType(queryPriority, session.getClientInfos());
    this.datasetValidityChecker = datasetValidityChecker;
    this.queryContextInfo =
        Utilities.createQueryContextInfo(
            session.getDefaultSchemaName(), priority, maxAllocation, session.getLastQueryId());
    this.contextInformation =
        new ContextInformationImpl(session.getCredentials(), queryContextInfo);

    this.allocator =
        sabotQueryContext
            .getQueryPlanningAllocator()
            .newChildAllocator(
                "query-planning:" + QueryIdHelper.getQueryId(queryId),
                plannerSettings.getInitialPlanningMemorySize(),
                plannerSettings.getPlanningMemoryLimit());
    this.bufferManager = new BufferManagerImpl(allocator);

    final String queryUserName = session.getCredentials().getUserName();
    final ViewExpansionContext viewExpansionContext =
        new ViewExpansionContext(CatalogUser.from(queryUserName));

    final SchemaConfig schemaConfig =
        SchemaConfig.newBuilder(CatalogUser.from(queryUserName))
            .defaultSchema(session.getDefaultSchemaPath())
            .optionManager(optionManager)
            .setViewExpansionContext(viewExpansionContext)
            .exposeInternalSources(session.exposeInternalSources())
            .setDatasetValidityChecker(datasetValidityChecker)
            .build();

    // Using caching namespace for query planning.  The lifecycle of the cache is associated with
    // the life cycle of
    // the Catalog.
    final ImmutableMetadataRequestOptions.Builder requestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setSourceVersionMapping(
                CaseInsensitiveMap.newImmutableMap(session.getSourceVersionMapping()))
            .setUseCachingNamespace(true)
            .setCheckValidity(session.checkMetadataValidity())
            .setNeverPromote(session.neverPromote())
            .setErrorOnUnspecifiedSourceVersion(
                ((priority != null) && (priority.getWorkloadType() == WorkloadType.ACCELERATOR))
                    || session.errorOnUnspecifiedVersion());

    final MetadataRequestOptions options = requestOptions.build();
    this.catalog = createCatalog(options);

    this.substitutionProviderFactory =
        sabotQueryContext
            .getConfig()
            .getInstance(
                "dremio.exec.substitution.factory",
                SubstitutionProviderFactory.class,
                DefaultSubstitutionProviderFactory.class);

    this.constantValueHolderCache = Maps.newHashMap();
    this.survivingRowCountsWithPruneFilter = Maps.newHashMap();
    this.survivingFileCountsWithPruneFilter = Maps.newHashMap();
    this.errorContexts = Lists.newArrayList();
    this.relMetadataQuerySupplier = sabotQueryContext.getRelMetadataQuerySupplier().get();
  }

  public PlannerCatalog createPlannerCatalog(
      ViewExpander viewExpander, OptionResolver optionResolver) {
    return new PlannerCatalogImpl(this, viewExpander, optionResolver);
  }

  @Override
  public CatalogService getCatalogService() {
    return sabotQueryContext.getCatalogService();
  }

  protected Catalog createCatalog(MetadataRequestOptions options) {
    return sabotQueryContext.getCatalogService().getCatalog(options);
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public MetadataStatsCollector getMetadataStatsCollector() {
    return getCatalog().getMetadataStatsCollector();
  }

  public UserDefinedFunctionCatalog getUserDefinedFunctionCatalog() {
    return new UserDefinedFunctionCatalogImpl(getOptions(), getNamespaceService(), getCatalog());
  }

  public AccelerationManager getAccelerationManager() {
    return sabotQueryContext.getAccelerationManager();
  }

  public ReflectionRoutingManager getReflectionRoutingManager() {
    return sabotQueryContext.getReflectionRoutingManager();
  }

  public RelMetadataQuerySupplier getRelMetadataQuerySupplier() {
    return relMetadataQuerySupplier;
  }

  public StatisticsService getStatisticsService() {
    return sabotQueryContext.getStatisticsService();
  }

  public StatisticsAdministrationService.Factory getStatisticsAdministrationFactory() {
    return sabotQueryContext.getStatisticsAdministrationFactoryProvider().get();
  }

  public SubstitutionProviderFactory getSubstitutionProviderFactory() {
    return substitutionProviderFactory;
  }

  public RuleSet getInjectedRules(PlannerPhase phase) {
    return RuleSets.ofList(
        sabotQueryContext.getInjectedRulesFactories().stream()
            .flatMap(rf -> rf.getRules(phase, optionManager).stream())
            .collect(Collectors.toList()));
  }

  public PlanCache getPlanCache() {
    return planCache;
  }

  @Override
  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public PlannerSettings getPlannerSettings() {
    return plannerSettings;
  }

  public UserSession getSession() {
    return session;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Get the user name of the user who issued the query that is managed by this QueryContext.
   *
   * @return
   */
  @Override
  public String getQueryUserName() {
    return session.getCredentials().getUserName();
  }

  /** Get the OptionManager for this context. */
  @Override
  public OptionManager getOptions() {
    return optionManager;
  }

  /**
   * Get the QueryOptionManager. Do not use unless use case specifically needs query options. If an
   * OptionManager is needed, use getOptions instead.
   */
  public QueryOptionManager getQueryOptionManager() {
    return queryOptionManager;
  }

  /**
   * Get the SessionOptionManager. Do not use unless use case specifically needs session options. If
   * an OptionManager is needed, use getOptions instead.
   */
  public SessionOptionManager getSessionOptionManager() {
    return session.getSessionOptionManager();
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  @Override
  public NodeEndpoint getCurrentEndpoint() {
    return sabotQueryContext.getEndpoint();
  }

  public LogicalPlanPersistence getLpPersistence() {
    return sabotQueryContext.getLpPersistence();
  }

  @Override
  public Collection<NodeEndpoint> getActiveEndpoints() {
    return sabotQueryContext.getExecutors();
  }

  public SabotConfig getConfig() {
    return sabotQueryContext.getConfig();
  }

  public DremioConfig getDremioConfig() {
    return sabotQueryContext.getDremioConfig();
  }

  public Provider<SimpleJobRunner> getJobsRunner() {
    return sabotQueryContext.getJobsRunner();
  }

  /**
   * Return the list of all non-default options including QUERY, SESSION and SYSTEM level
   *
   * @return
   */
  public OptionList getNonDefaultOptions() {
    return optionManager.getNonDefaultOptions();
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return functionImplementationRegistry;
  }

  public boolean isUserAuthenticationEnabled() {
    return sabotQueryContext.isUserAuthenticationEnabled();
  }

  public ScanResult getScanResult() {
    return sabotQueryContext.getClasspathScan();
  }

  public SqlOperatorTable getOperatorTable() {
    return table;
  }

  @Override
  public QueryContextInformation getQueryContextInfo() {
    return queryContextInfo;
  }

  @Override
  public ContextInformation getContextInformation() {
    return contextInformation;
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public PartitionExplorer getPartitionExplorer() {
    return new PartitionExplorerImpl(catalog);
  }

  @Override
  public EndPointListProvider getEndPointListProvider() {
    throw UserException.unsupportedError()
        .message("The end point list provider is not supported")
        .buildSilently();
  }

  @Override
  public int registerFunctionErrorContext(FunctionErrorContext errorContext) {
    assert errorContexts.size() == nextErrorContextId;
    errorContexts.add(errorContext);
    errorContext.setId(nextErrorContextId);
    nextErrorContextId++;
    return errorContext.getId();
  }

  @Override
  public FunctionErrorContext getFunctionErrorContext(int errorContextId) {
    assert 0 <= errorContextId && errorContextId <= errorContexts.size();
    return errorContexts.get(errorContextId);
  }

  @Override
  public FunctionErrorContext getFunctionErrorContext() {
    // Dummy context. TODO (DX-9622): remove this method once we handle the function interpretation
    // in the planning phase
    return FunctionErrorContextBuilder.builder().build();
  }

  @Override
  public int getFunctionErrorContextSize() {
    return errorContexts.size();
  }

  public MaterializationDescriptorProvider getMaterializationProvider() {
    return sabotQueryContext.getMaterializationProvider().get();
  }

  public Provider<WorkStats> getWorkStatsProvider() {
    return sabotQueryContext.getWorkStatsProvider();
  }

  public WorkloadType getWorkloadType() {
    return workloadType;
  }

  @Override
  public BufferManager getBufferManager() {
    return bufferManager;
  }

  @Override
  public ValueHolder getConstantValueHolder(
      String value, MinorType type, Function<ArrowBuf, ValueHolder> holderInitializer) {
    if (!constantValueHolderCache.containsKey(value)) {
      constantValueHolderCache.put(value, Maps.<MinorType, ValueHolder>newHashMap());
    }

    Map<MinorType, ValueHolder> holdersByType = constantValueHolderCache.get(value);
    ValueHolder valueHolder = holdersByType.get(type);
    if (valueHolder == null) {
      valueHolder = holderInitializer.apply(getManagedBuffer());
      holdersByType.put(type, valueHolder);
    }
    return valueHolder;
  }

  public void setGroupResourceInformation(GroupResourceInformation groupResourceInformation) {
    this.groupResourceInformation = groupResourceInformation;
  }

  public GroupResourceInformation getGroupResourceInformation() {
    return groupResourceInformation;
  }

  @Override
  public void close() throws Exception {
    try {
      if (!closed) {
        AutoCloseables.close(asList(bufferManager, allocator));
        session.setLastQueryId(queryId);
        // In case this QueryContext is going to live in the plan cache or materialization cache,
        // reduce the bloat
        CatalogUtil.clearAllDatasetCache(catalog);
      }
    } finally {
      closed = true;
    }
  }

  @Override
  public CompilationOptions getCompilationOptions() {
    return new CompilationOptions(optionManager);
  }

  public ExpressionSplitCache getExpressionSplitCache() {
    return sabotQueryContext.getExpressionSplitCache();
  }

  public ExecutorService getExecutorService() {
    return sabotQueryContext.getExecutorService();
  }

  public NamespaceService getNamespaceService() {
    return sabotQueryContext.getNamespaceService(getSession().getCredentials().getUserName());
  }

  public NamespaceService getSystemNamespaceService() {
    return sabotQueryContext.getNamespaceService(SYSTEM_USERNAME);
  }

  public boolean isCloud() {
    return !sabotQueryContext.getCoordinatorModeInfoProvider().get().isInSoftwareMode();
  }

  @WithSpan("QueryContext.getSurvivingRowCountWithPruneFilter")
  @Override
  public PartitionStatsValue getSurvivingRowCountWithPruneFilter(
      ScanRelBase scan, PruneFilterCondition pruneCondition) throws Exception {
    if (pruneCondition != null
        && getPlannerSettings().getOptions().getOption(ENABLE_PARTITION_STATS_USAGE)) {
      List<String> table = scan.getTableMetadata().getName().getPathComponents();
      if (!survivingRowCountsWithPruneFilter.containsKey(table)) {
        survivingRowCountsWithPruneFilter.put(table, Maps.newHashMap());
      }

      if (!survivingFileCountsWithPruneFilter.containsKey(table)) {
        survivingFileCountsWithPruneFilter.put(table, Maps.newHashMap());
      }

      Map<String, Long> fileCounts = survivingFileCountsWithPruneFilter.get(table);
      Map<String, Long> rowCounts = survivingRowCountsWithPruneFilter.get(table);
      String filterKey = pruneCondition.toString();

      Long rowCount = rowCounts.get(filterKey);
      Long fileCount = fileCounts.get(filterKey);

      if (rowCount == null) {
        Optional<PartitionStatsValue> stats =
            new PartitionStatsBasedPrunerCache(
                    this, scan, pruneCondition, partitionStatsPredicateCache)
                .lookupCache();

        if (stats.isPresent()) {
          rowCount = stats.get().getRowcount();
          fileCount = stats.get().getFilecount();
          rowCounts.put(filterKey, rowCount);
          fileCounts.put(filterKey, fileCount);
        } else {
          return null;
        }
      }
      return PartitionStatsValue.newBuilder().setRowcount(rowCount).setFilecount(fileCount).build();
    }
    return null;
  }

  public boolean getQueryRequiresGroupsInfo() {
    return queryRequiresGroupsInfo;
  }

  public void setQueryRequiresGroupsInfo(boolean queryRequiresGroupsInfo) {
    this.queryRequiresGroupsInfo = queryRequiresGroupsInfo;
  }

  public SabotQueryContext getSabotQueryContext() {
    return sabotQueryContext;
  }

  public PlannerNormalizerComponent createPlannerNormalizerComponent(
      PlannerBaseComponent plannerBaseComponent) {
    return PlannerNormalizerComponentImpl.build(
        plannerBaseComponent, new PlannerNormalizerModule());
  }
}
