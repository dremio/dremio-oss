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
package com.dremio.exec.ops;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ValueHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.calcite.schema.SchemaPlus;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.acceleration.substitution.DefaultSubstitutionProviderFactory;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProviderFactory;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.exec.server.MaterializationDescriptorProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionList;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.QueryOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.PartitionExplorer;
import com.dremio.exec.store.PartitionExplorerImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SchemaConfig.SchemaInfoProvider;
import com.dremio.exec.store.SchemaTreeProvider;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.util.Utilities;
import com.dremio.exec.work.WorkStats;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.exec.context.ContextInformation;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceService;
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import io.netty.buffer.ArrowBuf;

// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts
public class QueryContext implements AutoCloseable, OptimizerRulesContext {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final SabotContext sabotContext;
  private final UserSession session;
  private final OptionManager queryOptions;
  private final PlannerSettings plannerSettings;
  private final OperatorTable table;
  private final ExecutionControls executionControls;
  private final NamespaceService namespaceService;
  private final QueryId queryId;
  private final BufferAllocator allocator;
  private final BufferManager bufferManager;
  private final ContextInformation contextInformation;
  private final QueryContextInformation queryContextInfo;
  private final ViewExpansionContext viewExpansionContext;
  private final SchemaTreeProvider schemaTreeProvider;
  private final SubstitutionProviderFactory substitutionProviderFactory;
  private final SchemaInfoProvider infoProvider = new SchemaInfoProvider() {
    @Override
    public ViewExpansionContext getViewExpansionContext() {
      return viewExpansionContext;
    }

    @Override
    public OptionValue getOption(final String optionKey) {
      return getOptions().getOption(optionKey);
    }
  };
  /** Stores constants and their holders by type */
  private final Map<String, Map<MinorType, ValueHolder>> constantValueHolderCache;

  /*
   * Flag to indicate if close has been called, after calling close the first
   * time this is set to true and the close method becomes a no-op.
   */
  private boolean closed = false;

  public QueryContext(
      final UserSession session,
      final SabotContext sabotContext,
      QueryId queryId) {
    this(session, sabotContext, queryId, null, Long.MAX_VALUE);
  }
    public QueryContext(
      final UserSession session,
      final SabotContext sabotContext,
      QueryId queryId,
      QueryPriority priority,
      long maxAllocation) {
    this.sabotContext = sabotContext;
    this.session = session;
    queryOptions = new QueryOptionManager(session.getOptions());
    executionControls = new ExecutionControls(queryOptions, sabotContext.getEndpoint());
    plannerSettings = new PlannerSettings(queryOptions, getFunctionRegistry(), sabotContext.getClusterResourceInformation());
    plannerSettings.setNumEndPoints(sabotContext.getExecutors().size());
    table = new OperatorTable(getFunctionRegistry());

    queryContextInfo = Utilities.createQueryContextInfo(session.getDefaultSchemaName(), priority, maxAllocation);
    contextInformation = new ContextInformation(session.getCredentials(), queryContextInfo);
    this.queryId = queryId;
    allocator = sabotContext.getAllocator().newChildAllocator(
        "query-planing:" + QueryIdHelper.getQueryId(queryId),
        PlannerSettings.getInitialPlanningMemorySize(),
        plannerSettings.getPlanningMemoryLimit());
    bufferManager = new BufferManagerImpl(this.allocator);
    schemaTreeProvider = new SchemaTreeProvider(sabotContext);
    viewExpansionContext = new ViewExpansionContext(getSchemaInfoProvider(), schemaTreeProvider, session.getCredentials().getUserName());
    namespaceService = sabotContext.getNamespaceService(session.getCredentials().getUserName());
    constantValueHolderCache = Maps.newHashMap();
    substitutionProviderFactory = sabotContext.getConfig().getInstance("dremio.exec.substitution.factory",
        SubstitutionProviderFactory.class, DefaultSubstitutionProviderFactory.class);
  }

  public CatalogService getCatalogService(){
    return sabotContext.getCatalogService();
  }

  public AccelerationManager getAccelerationManager(){
    return sabotContext.getAccelerationManager();
  }

  public SubstitutionProviderFactory getSubstitutionProviderFactory() {
    return substitutionProviderFactory;
  }

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
   * Return reference to default schema instance in a schema tree. Each {@link org.apache.calcite.schema.SchemaPlus}
   * instance can refer to its parent and its children. From the returned reference to default schema instance,
   * clients can traverse the entire schema tree and know the default schema where to look up the tables first.
   *
   * @return Reference to default schema instance in a schema tree.
   */
  public SchemaPlus getNewDefaultSchema() {
    final SchemaPlus rootSchema = getRootSchema();
    final SchemaPlus defaultSchema = session.getDefaultSchema(rootSchema);
    if (defaultSchema == null) {
      return rootSchema;
    }

    return defaultSchema;
  }

  public SchemaPlus getRootSchema(boolean exposeSubSchemasAsTopLevelSchemas) {
    return getRootSchema(getQueryUserName(), exposeSubSchemasAsTopLevelSchemas);
  }

  /**
   * Get root schema with schema owner as the user who issued the query that is managed by this QueryContext.
   * @return Root of the schema tree.
   */
  public SchemaPlus getRootSchema() {
    return getRootSchema(getQueryUserName());
  }

  /**
   * Return root schema with schema owner as the given user.
   *
   * @param username User who owns the schema tree.
   * @return Root of the schema tree.
   */
  public SchemaPlus getRootSchema(final String username) {
    return getRootSchema(username, false);
  }

  private SchemaPlus getRootSchema(final String username, final boolean exposeSubSchemasAsTopLevelSchemas) {
    final SchemaConfig schemaConfig = SchemaConfig.newBuilder(username)
        .setProvider(getSchemaInfoProvider())
        .exposeSubSchemasAsTopLevelSchemas(exposeSubSchemasAsTopLevelSchemas)
        .exposeInternalSources(session.exposeInternalSources())
        .build();
    return schemaTreeProvider.getRootSchema(schemaConfig);
  }

  /**
   *  Create and return a SchemaTree with given <i>schemaConfig</i>.
   * @param schemaConfig
   * @return
   */
  public SchemaPlus getRootSchema(SchemaConfig schemaConfig) {
    return schemaTreeProvider.getRootSchema(schemaConfig);
  }

  /**
   * Get the user name of the user who issued the query that is managed by this QueryContext.
   * @return
   */
  public String getQueryUserName() {
    return session.getCredentials().getUserName();
  }

  public OptionManager getOptions() {
    return queryOptions;
  }

  public ExecutionControls getExecutionControls() {
    return executionControls;
  }

  public NodeEndpoint getCurrentEndpoint() {
    return sabotContext.getEndpoint();
  }

  public StoragePluginRegistry getStorage() {
    return sabotContext.getStorage();
  }

  public LogicalPlanPersistence getLpPersistence() {
    return sabotContext.getLpPersistence();
  }

  public Collection<NodeEndpoint> getActiveEndpoints() {
    return sabotContext.getExecutors();
  }

  public ClusterResourceInformation getClusterResourceInformation() {
    return sabotContext.getClusterResourceInformation();
  }

  public SabotConfig getConfig() {
    return sabotContext.getConfig();
  }

  public OptionList getNonDefaultSystemOptions() {
    return sabotContext.getOptionManager().getNonDefaultOptions();
  }

  @Override
  public FunctionImplementationRegistry getFunctionRegistry() {
    return sabotContext.getFunctionImplementationRegistry();
  }

  public SchemaInfoProvider getSchemaInfoProvider() {
    return infoProvider;
  }

  public SchemaTreeProvider getSchemaTreeProvider() {
    return schemaTreeProvider;
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
    return new PartitionExplorerImpl(getRootSchema());
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
      }
    } finally {
      closed = true;
    }
  }
}
