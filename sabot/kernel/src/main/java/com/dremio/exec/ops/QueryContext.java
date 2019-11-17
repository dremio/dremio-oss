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

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionErrorContextBuilder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.acceleration.substitution.DefaultSubstitutionProviderFactory;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.WorkloadType;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.PartitionExplorer;
import com.dremio.exec.store.PartitionExplorerImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.util.Utilities;
import com.dremio.exec.work.WorkStats;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
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

import io.netty.buffer.ArrowBuf;

// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts
public class QueryContext implements AutoCloseable, ResourceSchedulingContext, OptimizerRulesContext {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final SabotContext sabotContext;
  private final UserSession session;
  private final QueryId queryId;

  private final OptionManager queryOptions;
  private final ExecutionControls executionControls;
  private final PlannerSettings plannerSettings;
  private final OperatorTable table;

  private final QueryContextInformation queryContextInfo;
  protected ContextInformation contextInformation;

  private final BufferAllocator allocator;
  private final BufferManager bufferManager;

  private final Catalog catalog;
  private final NamespaceService namespaceService;
  private final SubstitutionProviderFactory substitutionProviderFactory;
  private final FunctionImplementationRegistry functionImplementationRegistry;

  /* Stores constants and their holders by type */
  private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;
  /* Stores error contexts registered with this function context **/
  private int nextErrorContextId = 0;
  private final List<FunctionErrorContext> errorContexts;
  protected final QueryPriority queryPriority;
  protected final Predicate<DatasetConfig> datasetValidityChecker;
  protected final WorkloadType workloadType;

  /*
   * Flag to indicate if close has been called, after calling close the first
   * time this is set to true and the close method becomes a no-op.
   */
  private boolean closed = false;

  public QueryContext(
      final UserSession session,
      final SabotContext sabotContext,
      QueryId queryId) {
    this(session, sabotContext, queryId, null, Long.MAX_VALUE, Predicates.<DatasetConfig>alwaysTrue());
  }

  public QueryContext(
      final UserSession session,
      final SabotContext sabotContext,
      QueryId queryId,
      QueryPriority priority,
      long maxAllocation,
      Predicate<DatasetConfig> datasetValidityChecker) {
    this.sabotContext = sabotContext;
    this.session = session;
    this.queryId = queryId;

    this.queryOptions = new QueryOptionManager(session.getOptions());
    this.executionControls = new ExecutionControls(queryOptions, sabotContext.getEndpoint());
    this.plannerSettings = new PlannerSettings(sabotContext.getConfig(), queryOptions,
        sabotContext.getClusterResourceInformation());
    this.plannerSettings.setNumEndPoints(sabotContext.getExecutors().size());
    functionImplementationRegistry = this.queryOptions.getOption(PlannerSettings
      .ENABLE_DECIMAL_V2)? sabotContext.getDecimalFunctionImplementationRegistry() : sabotContext
      .getFunctionImplementationRegistry();
    this.table = new OperatorTable(functionImplementationRegistry);

    this.queryPriority = priority;
    this.workloadType = Utilities.getWorkloadType(queryPriority, session.getClientInfos());
    this.datasetValidityChecker = datasetValidityChecker;
    this.queryContextInfo = Utilities.createQueryContextInfo(session.getDefaultSchemaName(), priority, maxAllocation, session.getLastQueryId());
    this.contextInformation = new ContextInformationImpl(session.getCredentials(), queryContextInfo);

    this.allocator = sabotContext.getQueryPlanningAllocator()
        .newChildAllocator("query-planning:" + QueryIdHelper.getQueryId(queryId),
            plannerSettings.getInitialPlanningMemorySize(),
            plannerSettings.getPlanningMemoryLimit());
    this.bufferManager = new BufferManagerImpl(allocator);

    final String queryUserName = session.getCredentials().getUserName();
    final ViewExpansionContext viewExpansionContext = new ViewExpansionContext(queryUserName);
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(queryUserName)
        .defaultSchema(session.getDefaultSchemaPath())
        .optionManager(queryOptions)
        .setViewExpansionContext(viewExpansionContext)
        .exposeInternalSources(session.exposeInternalSources())
        .setDatasetValidityChecker(datasetValidityChecker)
        .build();

    this.catalog = sabotContext.getCatalogService()
        .getCatalog(schemaConfig, Long.MAX_VALUE);
    this.namespaceService = sabotContext.getNamespaceService(queryUserName);
    this.substitutionProviderFactory = sabotContext.getConfig()
        .getInstance("dremio.exec.substitution.factory",
            SubstitutionProviderFactory.class,
            DefaultSubstitutionProviderFactory.class);

    this.constantValueHolderCache = Maps.newHashMap();
    this.errorContexts = Lists.newArrayList();
  }

  public CatalogService getCatalogService() {
    return sabotContext.getCatalogService();
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public AccelerationManager getAccelerationManager(){
    return sabotContext.getAccelerationManager();
  }

  public SubstitutionProviderFactory getSubstitutionProviderFactory() {
    return substitutionProviderFactory;
  }

  @Override
  public QueryId getQueryId(){
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
   * @return
   */
  @Override
  public String getQueryUserName() {
    return session.getCredentials().getUserName();
  }

  public OptionManager getOptions() {
    return queryOptions;
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  @Override
  public NodeEndpoint getCurrentEndpoint() {
    return sabotContext.getEndpoint();
  }

  public LogicalPlanPersistence getLpPersistence() {
    return sabotContext.getLpPersistence();
  }

  @Override
  public Collection<NodeEndpoint> getActiveEndpoints() {
    return sabotContext.getExecutors();
  }

  public ClusterResourceInformation getClusterResourceInformation() {
    return sabotContext.getClusterResourceInformation();
  }

  public SabotConfig getConfig() {
    return sabotContext.getConfig();
  }

  /**
   * Return the list of all non-default options including QUERY, SESSION and SYSTEM level
   * @return
   */
  public OptionList getNonDefaultOptions() {
    final OptionList nonDefaultOptions = queryOptions.getOptionList();
    nonDefaultOptions.mergeIfNotPresent(sabotContext.getOptionManager().getNonDefaultOptions());
    return nonDefaultOptions;
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return functionImplementationRegistry;
  }

  public boolean isUserAuthenticationEnabled() {
    return sabotContext.isUserAuthenticationEnabled();
  }

  public ScanResult getScanResult(){
    return sabotContext.getClasspathScan();
  }

  public OperatorTable getOperatorTable() {
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
    // Dummy context. TODO (DX-9622): remove this method once we handle the function interpretation in the planning phase
    return FunctionErrorContextBuilder.builder().build();
  }

  public MaterializationDescriptorProvider getMaterializationProvider() {
    return sabotContext.getMaterializationProvider().get();
  }

  public Provider<WorkStats> getWorkStatsProvider(){
    return sabotContext.getWorkStatsProvider();
  }

  public NamespaceService getNamespaceService() {
    return namespaceService;
  }

  public WorkloadType getWorkloadType() {
    return workloadType;
  }

  @Override
  public BufferManager getBufferManager() {
    return bufferManager;
  }

  @Override
  public ValueHolder getConstantValueHolder(String value, MinorType type, Function<ArrowBuf, ValueHolder> holderInitializer) {
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

  @Override
  public void close() throws Exception {
    try {
      if (!closed) {
        AutoCloseables.close(asList(bufferManager, allocator));
        session.setLastQueryId(queryId);
      }
    } finally {
      closed = true;
    }
  }

  @Override
  public CompilationOptions getCompilationOptions() {
    return new CompilationOptions(queryOptions);
  }
}
