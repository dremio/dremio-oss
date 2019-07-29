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
package com.dremio.exec.client;

import static com.dremio.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static com.dremio.exec.proto.UserProtos.RunQuery.newBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocatorFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.Version;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserProtos;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.GetQueryPlanFragments;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetSchemasResp;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaResp;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import com.dremio.exec.proto.UserProtos.LikeFilter;
import com.dremio.exec.proto.UserProtos.PreparedStatementHandle;
import com.dremio.exec.proto.UserProtos.Property;
import com.dremio.exec.proto.UserProtos.QueryPlanFragments;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.rpc.BasicClientWithConnection.ServerConnection;
import com.dremio.exec.rpc.ChannelClosedException;
import com.dremio.exec.rpc.ConnectionFailedException;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.rpc.RpcConnectionHandler;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.dremio.exec.rpc.TransportCheck;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.sabot.rpc.user.UserClient;
import com.dremio.sabot.rpc.user.UserResultsListener;
import com.dremio.sabot.rpc.user.UserRpcUtils;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.ElectionListener;
import com.dremio.service.coordinator.ElectionRegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.dremio.service.coordinator.zk.ZKClusterCoordinator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractCheckedFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

/**
 * Thin wrapper around a UserClient that handles connect/close and transforms
 * String into ByteBuf.
 */
public class DremioClient implements Closeable, ConnectionThrottle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioClient.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  // lower case properties that should not be sent to the server
  private static final ImmutableSet<String> CLIENT_ONLY_PROPERTIES =
      ImmutableSet.<String>builder()
          .add(SSLConfig.TRUST_STORE_PASSWORD.toLowerCase(Locale.ROOT))
          .add(SSLConfig.TRUST_STORE_PATH.toLowerCase(Locale.ROOT))
          .add(SSLConfig.TRUST_STORE_TYPE.toLowerCase(Locale.ROOT))
          .add(SSLConfig.DISABLE_CERT_VERIFICATION.toLowerCase(Locale.ROOT))
          .add(SSLConfig.DISABLE_HOST_VERIFICATION.toLowerCase(Locale.ROOT))
          .add(SSLConfig.ENABLE_SSL.toLowerCase(Locale.ROOT))
          .build();

  // A wrapper class which ignore calls to close()
  private static class ClusterCoordinatorWrapper extends ClusterCoordinator {
    private final ClusterCoordinator clusterCoordinator;

    public ClusterCoordinatorWrapper(ClusterCoordinator clusterCoordinator) {
      this.clusterCoordinator = clusterCoordinator;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public ServiceSet getServiceSet(Role role) {
      return clusterCoordinator.getServiceSet(role);
    }

    @Override
    public ServiceSet getOrCreateServiceSet(String serviceName) {
      return clusterCoordinator.getOrCreateServiceSet(serviceName);
    }

    @Override
    public Iterable<String> getServiceNames() throws Exception {
      return clusterCoordinator.getServiceNames();
    }

    @Override
    public DistributedSemaphore getSemaphore(String name, int maximumLeases) {
      throw new UnsupportedOperationException("registerService is not supported in client");
    }

    @Override
    public ElectionRegistrationHandle joinElection(String name, ElectionListener listener) {
      throw new UnsupportedOperationException("registerService is not supported in client");
    }

    @Override
    public void close() {
    }

    private static ClusterCoordinator of(ClusterCoordinator coordinator) {
      if (coordinator == null) {
        return null;
      }
      return new ClusterCoordinatorWrapper(coordinator);
    }
  }

  public static final String DEFAULT_CLIENT_NAME = "Dremio Java client";

  private final SabotConfig config;
  private UserClient client;
  private UserProperties props = null;
  private volatile ClusterCoordinator clusterCoordinator;
  private volatile boolean connected = false;
  private final BufferAllocator rootAllocator;
  private final BufferAllocator connectionAllocator;
  private final BufferAllocator recordAllocator;
  private int reconnectTimes;
  private int reconnectDelay;
  private boolean supportComplexTypes;
  private final boolean ownsAllocator;
  private final boolean isDirectConnection; // true if the connection bypasses zookeeper and connects directly to a node
  private EventLoopGroup eventLoopGroup;
  private ExecutorService executor;
  private String clientName = DEFAULT_CLIENT_NAME;


  public DremioClient() throws OutOfMemoryException {
    this(SabotConfig.create(), false);
  }

  public DremioClient(boolean isDirect) throws OutOfMemoryException {
    this(SabotConfig.create(), isDirect);
  }

  public DremioClient(String fileName) throws OutOfMemoryException {
    this(SabotConfig.create(fileName), false);
  }

  public DremioClient(SabotConfig config) throws OutOfMemoryException {
    this(config, null, false);
  }

  public DremioClient(SabotConfig config, boolean isDirect)
      throws OutOfMemoryException {
    this(config, null, isDirect);
  }

  public DremioClient(SabotConfig config, ClusterCoordinator coordinator)
    throws OutOfMemoryException {
    this(config, coordinator, null, false);
  }

  public DremioClient(SabotConfig config, ClusterCoordinator coordinator, boolean isDirect)
    throws OutOfMemoryException {
    this(config, coordinator, null, isDirect);
  }

  public DremioClient(SabotConfig config, ClusterCoordinator coordinator, BufferAllocator allocator)
      throws OutOfMemoryException {
    this(config, coordinator, allocator, false);
  }

  public DremioClient(SabotConfig config, ClusterCoordinator clusterCoordinator, BufferAllocator allocator, boolean isDirect) {
    // if isDirect is true, the client will connect directly to the node instead of
    // going thru the zookeeper
    this.isDirectConnection = isDirect;
    this.ownsAllocator = allocator == null;
    this.rootAllocator = ownsAllocator ? RootAllocatorFactory.newRoot(config) : allocator;
    this.connectionAllocator = rootAllocator.newChildAllocator("dremio-client-connections", 0, rootAllocator.getLimit());
    this.recordAllocator = rootAllocator.newChildAllocator("dremio-client-records", 0, rootAllocator.getLimit());
    this.config = config;
    this.clusterCoordinator = ClusterCoordinatorWrapper.of(clusterCoordinator);
    this.reconnectTimes = config.getInt(ExecConstants.BIT_RETRY_TIMES);
    this.reconnectDelay = config.getInt(ExecConstants.BIT_RETRY_DELAY);
    this.supportComplexTypes = config.getBoolean(ExecConstants.CLIENT_SUPPORT_COMPLEX_TYPES);
  }

  public SabotConfig getConfig() {
    return config;
  }

  @Override
  public void setAutoRead(boolean enableAutoRead) {
    client.setAutoRead(enableAutoRead);
  }

  /**
   * Sets the client name.
   *
   * If not set, default is {@code DremioClient#DEFAULT_CLIENT_NAME}.
   *
   * @param name the client name
   *
   * @throws IllegalStateException if called after a connection has been established.
   * @throws NullPointerException if client name is null
   */
  public void setClientName(String name) {
    if (connected) {
      throw new IllegalStateException("Attempted to modify client connection property after connection has been established.");
    }
    this.clientName = checkNotNull(name, "client name should not be null");
  }

  /**
   * Sets whether the application is willing to accept complex types (Map, Arrays) in the returned result set.
   * Default is {@code true}. If set to {@code false}, the complex types are returned as JSON encoded VARCHAR type.
   *
   * @throws IllegalStateException if called after a connection has been established.
   */
  public void setSupportComplexTypes(boolean supportComplexTypes) {
    if (connected) {
      throw new IllegalStateException("Attempted to modify client connection property after connection has been established.");
    }
    this.supportComplexTypes = supportComplexTypes;
  }

  /**
   * Connects the client to a SabotNode server
   *
   * @throws RpcException
   */
  public void connect() throws RpcException {
    connect(null, null);
  }

  public void connect(Properties props) throws RpcException {
    connect(null, props);
  }

  public synchronized void connect(String connect, Properties props) throws RpcException {
    if (connected) {
      return;
    }

    final NodeEndpoint endpoint;
    if (isDirectConnection) {
      final String[] connectInfo = props.getProperty("direct").split(":");
      final String port = connectInfo.length==2?connectInfo[1]:config.getString(ExecConstants.INITIAL_USER_PORT);
      endpoint = NodeEndpoint.newBuilder()
              .setAddress(connectInfo[0])
              .setUserPort(Integer.parseInt(port))
              .build();
    } else {
      if (clusterCoordinator == null) {
        try {
          clusterCoordinator = new ZKClusterCoordinator(this.config, connect);
          clusterCoordinator.start();
        } catch (Exception e) {
          throw new RpcException("Failure setting up ZK for client.", e);
        }
      }

      final ArrayList<NodeEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).getAvailableEndpoints());
      checkState(!endpoints.isEmpty(), "No NodeEndpoint can be found");
      // shuffle the collection then get the first endpoint
      Collections.shuffle(endpoints);
      endpoint = endpoints.iterator().next();
    }

    if (props != null) {
      final UserProperties.Builder upBuilder = UserProperties.newBuilder();
      for (final String key : props.stringPropertyNames()) {
        if (CLIENT_ONLY_PROPERTIES.contains(key.toLowerCase(Locale.ROOT))) {
          continue;
        }

        upBuilder.addProperties(
            Property.newBuilder()
                .setKey(key)
                .setValue(props.getProperty(key)));
      }

      this.props = upBuilder.build();
    }

    eventLoopGroup = createEventLoop(config.getInt(ExecConstants.CLIENT_RPC_THREADS), "Client-");
    executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("dremio-client-executor-")) {
      @Override
      protected void afterExecute(final Runnable r, final Throwable t) {
        if (t != null) {
          logger.error("{}.run() leaked an exception.", r.getClass().getName(), t);
        }
        super.afterExecute(r, t);
      }
    };
    client = new UserClient(clientName, config, supportComplexTypes, connectionAllocator, eventLoopGroup, executor,
        SSLConfig.of(props));
    logger.debug("Connecting to server {}:{}", endpoint.getAddress(), endpoint.getUserPort());
    connect(endpoint);
    connected = true;
  }

  protected static EventLoopGroup createEventLoop(int size, String prefix) {
    return TransportCheck.createEventLoopGroup(size, prefix);
  }

  public synchronized boolean reconnect() {
    if (client.isActive()) {
      return true;
    }
    int retry = reconnectTimes;
    while (retry > 0) {
      retry--;
      try {
        Thread.sleep(this.reconnectDelay);
        final ArrayList<NodeEndpoint> endpoints = new ArrayList<>(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR).getAvailableEndpoints());
        if (endpoints.isEmpty()) {
          continue;
        }
        client.close();
        Collections.shuffle(endpoints);
        connect(endpoints.iterator().next());
        return true;
      } catch (Exception e) {
      }
    }
    return false;
  }

  private void connect(NodeEndpoint endpoint) throws RpcException {
    final FutureHandler f = new FutureHandler();
    client.connect(f, endpoint, props, getUserCredentials());
    try {
      f.checkedGet(30, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new RpcException("Timed out after 30s waiting to connect to "+endpoint.getAddress() +":"+endpoint.getUserPort());
    }
  }

  public BufferAllocator getRecordAllocator() {
    return recordAllocator;
  }

  /**
   * Closes this client's connection to the server
   */
  @Override
  public void close() {
    if (this.client != null) {
      this.client.close();
    }
    AutoCloseables.closeNoChecked(recordAllocator);
    AutoCloseables.closeNoChecked(connectionAllocator);
    if (this.ownsAllocator && rootAllocator != null) {
      AutoCloseables.closeNoChecked(rootAllocator);
    }

    if (clusterCoordinator != null) {
      try {
        clusterCoordinator.close();
        clusterCoordinator = null;
      } catch (Exception e) {
        logger.warn("Error while closing Cluster Coordinator.", e);
      }
    }

    if (eventLoopGroup != null) {
      try {
        eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
      } catch (InterruptedException e) {
        logger.warn("Failure while shutting down event loop in dremio client. ", e);
        Thread.interrupted();
      }
    }

    if (executor != null) {
      executor.shutdownNow();
    }

    // TODO:  Did DRILL-1735 changes cover this TODO?:
    // TODO: fix tests that fail when this is called.
    //allocator.close();
    connected = false;
  }

  /**
   * Return the server name. Only available after connecting
   *
   * The result might be null if the server doesn't provide the name information.
   *
   * @return the server name, or null if not connected or if the server
   *         doesn't provide the name
   * @return
   */
  public String getServerName() {
    return (client != null && client.getServerInfos() != null) ? client.getServerInfos().getName() : null;
  }

  /**
   * Return the server version. Only available after connecting
   *
   * The result might be null if the server doesn't provide the version information.
   *
   * @return the server version, or null if not connected or if the server
   *         doesn't provide the version
   * @return
   */
  public Version getServerVersion() {
    return (client != null && client.getServerInfos() != null) ? UserRpcUtils.getVersion(client.getServerInfos()) : null;
  }


  /**
   * Get server meta information
   *
   * Get meta information about the server like the the available functions
   * or the identifier quoting string used by the current session
   *
   * @return a future to the server meta response
   */
  public RpcFuture<GetServerMetaResp> getServerMeta() {
    return client.send(RpcType.GET_SERVER_META, GetServerMetaReq.getDefaultInstance(), GetServerMetaResp.class);
  }

  /**
   * Returns the list of methods supported by the server based on its advertised information.
   *
   * @return a immutable set of capabilities
   */
  public Set<ServerMethod> getSupportedMethods() {
    return client != null ? ServerMethod.getSupportedMethods(client.getSupportedMethods(), client.getServerInfos()) : null;
  }


  /**
   * Submits a string based query plan for execution and returns the result batches. Supported query types are:
   * <p><ul>
   *  <li>{@link QueryType#LOGICAL}
   *  <li>{@link QueryType#PHYSICAL}
   *  <li>{@link QueryType#SQL}
   * </ul>
   *
   * @param type Query type
   * @param plan Query to execute
   * @return a handle for the query result
   * @throws RpcException
   */
  public List<QueryDataBatch> runQuery(QueryType type, String plan) throws RpcException {
    checkArgument(type == QueryType.LOGICAL || type == QueryType.PHYSICAL || type == QueryType.SQL,
        String.format("Only query types %s, %s and %s are supported in this API",
            QueryType.LOGICAL, QueryType.PHYSICAL, QueryType.SQL));
    final UserProtos.RunQuery query = newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build();
    final ListHoldingResultsListener listener = new ListHoldingResultsListener(query);
    client.submitQuery(listener, query);
    return listener.getResults();
  }

  /**
   * API to just plan a query without execution
   * @param type
   * @param query
   * @param isSplitPlan - option to tell whether to return single or split plans for a query
   * @return list of PlanFragments that can be used later on in {@link #runQuery(QueryType, List, UserResultsListener)}
   * to run a query without additional planning
   */
  public RpcFuture<QueryPlanFragments> planQuery(QueryType type, String query, boolean isSplitPlan) {
    GetQueryPlanFragments runQuery = GetQueryPlanFragments.newBuilder().setQuery(query).setType(type).setSplitPlan(isSplitPlan).build();
    return client.planQuery(runQuery);
  }

  /**
   * Run query based on list of fragments that were supposedly produced during query planning phase. Supported
   * query type is {@link QueryType#EXECUTION}
   * @param type
   * @param planFragments
   * @param resultsListener
   * @throws RpcException
   */
  public void runQuery(QueryType type, PlanFragmentSet planFragments, UserResultsListener resultsListener)
      throws RpcException {
    // QueryType can be only executional
    checkArgument((QueryType.EXECUTION == type), "Only EXECUTION type query is supported with PlanFragments");

    final UserProtos.RunQuery query = newBuilder().setType(type)
      .setFragmentSet(planFragments)
      .setResultsMode(STREAM_FULL)
      .build();
    client.submitQuery(resultsListener, query);
  }

  /*
   * Helper method to generate the UserCredentials message from the properties.
   */
  private UserBitShared.UserCredentials getUserCredentials() {
    // If username is not propagated as one of the properties
    String userName = "anonymous";

    if (props != null) {
      for (Property property: props.getPropertiesList()) {
        if (property.getKey().equalsIgnoreCase("user") && !Strings.isNullOrEmpty(property.getValue())) {
          userName = property.getValue();
          break;
        }
      }
    }

    return UserBitShared.UserCredentials.newBuilder().setUserName(userName).build();
  }

  public RpcFuture<Ack> cancelQuery(QueryId id) {
    if(logger.isDebugEnabled()) {
      logger.debug("Cancelling query {}", QueryIdHelper.getQueryId(id));
    }
    return client.send(RpcType.CANCEL_QUERY, id, Ack.class);
  }

  public RpcFuture<Ack> resumeQuery(final QueryId queryId) {
    if(logger.isDebugEnabled()) {
      logger.debug("Resuming query {}", QueryIdHelper.getQueryId(queryId));
    }
    return client.send(RpcType.RESUME_PAUSED_QUERY, queryId, Ack.class);
  }

  /**
   * Get the list of catalogs in <code>INFORMATION_SCHEMA.CATALOGS</code> table satisfying the given filters.
   *
   * @param catalogNameFilter Filter on <code>catalog name</code>. Pass null to apply no filter.
   * @return
   */
  public RpcFuture<GetCatalogsResp> getCatalogs(LikeFilter catalogNameFilter) {
    final GetCatalogsReq.Builder reqBuilder = GetCatalogsReq.newBuilder();
    if (catalogNameFilter != null) {
      reqBuilder.setCatalogNameFilter(catalogNameFilter);
    }

    return client.send(RpcType.GET_CATALOGS, reqBuilder.build(), GetCatalogsResp.class);
  }

  /**
   * Get the list of schemas in <code>INFORMATION_SCHEMA.SCHEMATA</code> table satisfying the given filters.
   *
   * @param catalogNameFilter Filter on <code>catalog name</code>. Pass null to apply no filter.
   * @param schemaNameFilter Filter on <code>schema name</code>. Pass null to apply no filter.
   * @return
   */
  public RpcFuture<GetSchemasResp> getSchemas(LikeFilter catalogNameFilter, LikeFilter schemaNameFilter) {
    final GetSchemasReq.Builder reqBuilder = GetSchemasReq.newBuilder();
    if (catalogNameFilter != null) {
      reqBuilder.setCatalogNameFilter(catalogNameFilter);
    }

    if (schemaNameFilter != null) {
      reqBuilder.setSchemaNameFilter(schemaNameFilter);
    }

    return client.send(RpcType.GET_SCHEMAS, reqBuilder.build(), GetSchemasResp.class);
  }

  /**
   * Get the list of tables in <code>INFORMATION_SCHEMA.TABLES</code> table satisfying the given filters.
   *
   * @param catalogNameFilter Filter on <code>catalog name</code>. Pass null to apply no filter.
   * @param schemaNameFilter Filter on <code>schema name</code>. Pass null to apply no filter.
   * @param tableNameFilter Filter in <code>table name</code>. Pass null to apply no filter.
   * @param tableTypeFilter Filter in <code>table type</code>. Pass null to apply no filter.
   * @return
   */
  public RpcFuture<GetTablesResp> getTables(LikeFilter catalogNameFilter, LikeFilter schemaNameFilter,
      LikeFilter tableNameFilter, List<String> tableTypeFilter) {
    final GetTablesReq.Builder reqBuilder = GetTablesReq.newBuilder();
    if (catalogNameFilter != null) {
      reqBuilder.setCatalogNameFilter(catalogNameFilter);
    }

    if (schemaNameFilter != null) {
      reqBuilder.setSchemaNameFilter(schemaNameFilter);
    }

    if (tableNameFilter != null) {
      reqBuilder.setTableNameFilter(tableNameFilter);
    }

    if (tableTypeFilter != null) {
      reqBuilder.addAllTableTypeFilter(tableTypeFilter);
    }

    return client.send(RpcType.GET_TABLES, reqBuilder.build(), GetTablesResp.class);
  }

  /**
   * Get the list of columns in <code>INFORMATION_SCHEMA.COLUMNS</code> table satisfying the given filters.
   *
   * @param catalogNameFilter Filter on <code>catalog name</code>. Pass null to apply no filter.
   * @param schemaNameFilter Filter on <code>schema name</code>. Pass null to apply no filter.
   * @param tableNameFilter Filter in <code>table name</code>. Pass null to apply no filter.
   * @param columnNameFilter Filter in <code>column name</code>. Pass null to apply no filter.
   * @return
   */
  public RpcFuture<GetColumnsResp> getColumns(LikeFilter catalogNameFilter, LikeFilter schemaNameFilter,
      LikeFilter tableNameFilter, LikeFilter columnNameFilter) {
    final GetColumnsReq.Builder reqBuilder = GetColumnsReq.newBuilder();
    if (catalogNameFilter != null) {
      reqBuilder.setCatalogNameFilter(catalogNameFilter);
    }

    if (schemaNameFilter != null) {
      reqBuilder.setSchemaNameFilter(schemaNameFilter);
    }

    if (tableNameFilter != null) {
      reqBuilder.setTableNameFilter(tableNameFilter);
    }

    if (columnNameFilter != null) {
      reqBuilder.setColumnNameFilter(columnNameFilter);
    }

    return client.send(RpcType.GET_COLUMNS, reqBuilder.build(), GetColumnsResp.class);
  }

  /**
   * Create a prepared statement for given <code>query</code>.
   *
   * @param query
   * @return
   */
  public RpcFuture<CreatePreparedStatementResp> createPreparedStatement(final String query) {
    final CreatePreparedStatementReq req =
        CreatePreparedStatementReq.newBuilder()
            .setSqlQuery(query)
            .build();

    return client.send(RpcType.CREATE_PREPARED_STATEMENT, req, CreatePreparedStatementResp.class);
  }

  /**
   * Execute the given prepared statement.
   *
   * @param preparedStatementHandle Prepared statement handle returned in response to
   *                                {@link #createPreparedStatement(String)}.
   * @param resultsListener {@link UserResultsListener} instance for listening for query results.
   */
  public void executePreparedStatement(final PreparedStatementHandle preparedStatementHandle,
      final UserResultsListener resultsListener) {
    final RunQuery runQuery = newBuilder()
        .setResultsMode(STREAM_FULL)
        .setType(QueryType.PREPARED_STATEMENT)
        .setPreparedStatementHandle(preparedStatementHandle)
        .build();
    client.submitQuery(resultsListener, runQuery);
  }

  /**
   * Execute the given prepared statement and return the results.
   *
   * @param preparedStatementHandle Prepared statement handle returned in response to
   *                                {@link #createPreparedStatement(String)}.
   * @return List of {@link QueryDataBatch}s. It is responsibility of the caller to release query data batches.
   * @throws RpcException
   */
  public List<QueryDataBatch> executePreparedStatement(final PreparedStatementHandle preparedStatementHandle)
      throws RpcException {
    final RunQuery runQuery = newBuilder()
        .setResultsMode(STREAM_FULL)
        .setType(QueryType.PREPARED_STATEMENT)
        .setPreparedStatementHandle(preparedStatementHandle)
        .build();

    final ListHoldingResultsListener resultsListener = new ListHoldingResultsListener(runQuery);

    client.submitQuery(resultsListener, runQuery);

    return resultsListener.getResults();
  }

  /**
   * Submits a Logical plan for direct execution (bypasses parsing)
   *
   * @param  plan  the plan to execute
   */
  public void runQuery(QueryType type, String plan, UserResultsListener resultsListener) {
    client.submitQuery(resultsListener, newBuilder().setResultsMode(STREAM_FULL).setType(type).setPlan(plan).build());
  }

  private class ListHoldingResultsListener implements UserResultsListener {
    private final Vector<QueryDataBatch> results = new Vector<>();
    private final SettableFuture<List<QueryDataBatch>> future = SettableFuture.create();
    private final UserProtos.RunQuery query ;

    public ListHoldingResultsListener(UserProtos.RunQuery query) {
      logger.debug( "Listener created for query \"\"\"{}\"\"\"", query );
      this.query = query;
    }

    @Override
    public void submissionFailed(UserException ex) {
      // or  !client.isActive()
      if (ex.getCause() instanceof ChannelClosedException) {
        if (reconnect()) {
          try {
            client.submitQuery(this, query);
          } catch (Exception e) {
            fail(e);
          }
        } else {
          fail(ex);
        }
      } else {
        fail(ex);
      }
    }

    @Override
    public void queryCompleted(QueryState state) {
      future.set(results);
    }

    private void fail(Exception ex) {
      logger.debug("Submission failed.", ex);
      future.setException(ex);
      future.set(results);
    }

    @Override
    public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
      logger.debug("Result arrived:  Result: {}", result );
      results.add(result);
    }

    public List<QueryDataBatch> getResults() throws RpcException{
      try {
        return future.get();
      } catch (Throwable t) {
        /*
         * Since we're not going to return the result to the caller
         * to clean up, we have to do it.
         */
        for(final QueryDataBatch queryDataBatch : results) {
          queryDataBatch.release();
        }

        throw RpcException.mapException(t);
      }
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
      if (logger.isDebugEnabled()) {
        logger.debug("Query ID arrived: {}", QueryIdHelper.getQueryId(queryId));
      }
    }
  }

  private class FutureHandler extends AbstractCheckedFuture<Void, RpcException> implements RpcConnectionHandler<ServerConnection>, RpcFuture<Void>{
    protected FutureHandler() {
      super( SettableFuture.<Void>create());
    }

    @Override
    public void connectionSucceeded(ServerConnection connection) {
      getInner().set(null);
    }

    @Override
    public void connectionFailed(FailureType type, Throwable t) {
      getInner().setException(ConnectionFailedException.mapException(
        new RpcException(String.format("%s : %s", type.name(), t.getMessage()), t), type));
    }

    private SettableFuture<Void> getInner() {
      return (SettableFuture<Void>) delegate();
    }

    @Override
    protected RpcException mapException(Exception e) {
      return RpcException.mapException(e);
    }

    @Override
    public ByteBuf getBuffer() {
      return null;
    }
  }
}
