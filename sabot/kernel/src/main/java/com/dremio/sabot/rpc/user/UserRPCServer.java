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
package com.dremio.sabot.rpc.user;

import static com.dremio.exec.proto.UserProtos.CreatePreparedStatementArrowReq;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import javax.inject.Provider;
import javax.net.ssl.SSLException;

import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.avatica.util.Quoting;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryWritableBatch;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProvider;
import com.dremio.exec.planner.sql.handlers.commands.PreparedStatementProvider;
import com.dremio.exec.planner.sql.handlers.commands.ServerMetaProvider;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
import com.dremio.exec.proto.UserBitShared.ExternalId;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.dremio.exec.proto.UserProtos.BitToUserHandshake;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.HandshakeStatus;
import com.dremio.exec.proto.UserProtos.Property;
import com.dremio.exec.proto.UserProtos.RecordBatchFormat;
import com.dremio.exec.proto.UserProtos.RecordBatchType;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.proto.UserProtos.UserToBitHandshake;
import com.dremio.exec.rpc.BasicServer;
import com.dremio.exec.rpc.MessageDecoder;
import com.dremio.exec.rpc.OutboundRpcMessage;
import com.dremio.exec.rpc.RemoteConnection;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.RpcCompatibilityEncoder;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.exec.server.options.SessionOptionManager;
import com.dremio.exec.server.options.SessionOptionManagerFactory;
import com.dremio.exec.server.options.SessionOptionManagerFactoryImpl;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.protector.UserConnectionResponseHandler;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;

public class UserRPCServer extends BasicServer<RpcType, UserRPCServer.UserClientConnectionImpl> {
  private static final org.slf4j.Logger CONNECTION_LOGGER = org.slf4j.LoggerFactory.getLogger("com.dremio.ConnectionLog");
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserRPCServer.class);

  private static final String RPC_COMPATIBILITY_ENCODER = "rpc-compatibility-encoder";
  private static final String DRILL_COMPATIBILITY_ENCODER = "backward-compatibility-encoder";

  private final Provider<UserService> userServiceProvider;
  private final Provider<NodeEndpoint> nodeEndpointProvider;

  private final WorkIngestor workIngestor;
  private final Provider<UserWorker> worker;
  private final InboundImpersonationManager impersonationManager;
  private final BufferAllocator allocator;
  private final SessionOptionManagerFactory sessionOptionManagerFactory;
  private final Tracer tracer;

  @VisibleForTesting
  UserRPCServer(
      RpcConfig rpcConfig,
      Provider<UserService> userServiceProvider,
      Provider<NodeEndpoint> nodeEndpointProvider,
      WorkIngestor workIngestor,
      Provider<UserWorker> worker,
      BufferAllocator allocator,
      EventLoopGroup eventLoopGroup,
      InboundImpersonationManager impersonationManager,
      Tracer tracer,
      OptionValidatorListing optionValidatorListing
      ) {
    super(rpcConfig, new ArrowByteBufAllocator(allocator), eventLoopGroup);
    this.userServiceProvider = userServiceProvider;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.workIngestor = workIngestor;
    this.worker = worker;
    this.allocator = allocator;
    this.impersonationManager = impersonationManager;
    this.sessionOptionManagerFactory = new SessionOptionManagerFactoryImpl(optionValidatorListing);
    this.tracer = tracer;
  }

  UserRPCServer(
    RpcConfig rpcConfig,
    Provider<UserService> userServiceProvider,
    Provider<NodeEndpoint> nodeEndpointProvider,
    Provider<UserWorker> worker,
    BufferAllocator allocator,
    EventLoopGroup eventLoopGroup,
    InboundImpersonationManager impersonationManager,
    Tracer tracer,
    OptionValidatorListing optionValidatorListing
  ) {
    this(rpcConfig, userServiceProvider, nodeEndpointProvider, new WorkIngestorImpl(worker), worker, allocator, eventLoopGroup, impersonationManager, tracer, optionValidatorListing);
  }

  @Override
  protected void initChannel(final SocketChannel ch) throws SSLException {
    // configure the main pipeline
    super.initChannel(ch);
    ch.pipeline().addLast(RPC_COMPATIBILITY_ENCODER, new RpcCompatibilityEncoder());
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    // a user server only expects acknowledgments on messages it creates.
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected Response handle(UserClientConnectionImpl connection, int rpcType, byte[] pBody, ByteBuf dBody)
      throws RpcException {
    // there are two #handle methods, since the other one is overridden, this is never invoked
    throw new IllegalStateException("UserRPCServer#handle must not be invoked without ResponseSender");
  }

  @Override
  protected void handle(UserClientConnectionImpl connection, int rpcType, byte[] pBody, ByteBuf dBody,
      ResponseSender responseSender) throws RpcException {

    final Span span;
    if(connection.getSession().isTracingEnabled()) {
      span = tracer.buildSpan("user_rpc_request").start();
    }
    else {
      span = NoopSpan.INSTANCE;
    }

    ResponseSender wrappedSender = new ResponseSender() {
      @Override
      public void send(Response r) {
        responseSender.send(r);
        span.finish();
      }

      @Override
      public void sendFailure(UserRpcException e) {
        responseSender.sendFailure(e);
        span.finish();
      }
    };

    span.setTag("rpc_type", RpcType.valueOf(rpcType).toString());
    // If there's an error parsing the request, the failure will not be sent through the response sender.
    try (Scope scope = tracer.activateSpan(span)){
      workIngestor.feedWork(connection, rpcType, pBody, dBody, wrappedSender);
    } catch (RpcException e){
      span.finish();
      throw e;
    }
  }

  /**
   * Choose the best Dremio record batch format based on client capabilities.
   *
   * @param inbound
   * @return the chosen format
   * @throws IllegalArgumentException if record batch format or type is unknown or invalid
   */
  @VisibleForTesting
  static RecordBatchFormat chooseDremioRecordBatchFormat(UserToBitHandshake inbound) {
    Preconditions.checkArgument(inbound.getRecordBatchType() == RecordBatchType.DREMIO);

    // Choose record batch format based on what's supported by both clients and servers
    List<RecordBatchFormat> supportedRecordBatchFormatsList = inbound.getSupportedRecordBatchFormatsList();
    if (supportedRecordBatchFormatsList.isEmpty()) {
      supportedRecordBatchFormatsList = ImmutableList.of(RecordBatchFormat.DREMIO_0_9); // Backward compatibility with older dremio clients
    }

    try {
      RecordBatchFormat recordBatchFormat = Ordering
          .explicit(RecordBatchFormat.UNKNOWN, RecordBatchFormat.DREMIO_0_9, RecordBatchFormat.DREMIO_1_4)
          .max(supportedRecordBatchFormatsList);

      // If unknown is the max value, it means client only supports format we don't know about yet.
      if (recordBatchFormat == RecordBatchFormat.UNKNOWN) {
        throw new IllegalArgumentException("Record batch format not supported");
      }
      return recordBatchFormat;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException("Invalid batch format received", e);
    }
  }

  @VisibleForTesting
  SessionOptionManagerFactory getSessionOptionManagerFactory() {
    return sessionOptionManagerFactory;
  }

  /**
   * Interface for getting user session properties and interacting with user connection. Separating this interface from
   * {@link RemoteConnection} implementation for user connection:
   * <p><ul>
   *   <li> Connection is passed to AttemptManager and Screen operators. Instead passing this interface exposes few details.
   *   <li> Makes it easy to have wrappers around user connection which can be helpful to tap the messages and data
   *        going to the actual client.
   * </ul>
   */
  public interface UserClientConnection extends TerminationListenerRegistry {
    /**
     * @return User session object.
     */
    UserSession getSession();

    /**
     * Send query result outcome to client. Outcome is returned through <code>listener</code>
     * @param listener
     * @param result
     */
    void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result);

    /**
     * Send query data to client. Outcome is returned through <code>listener</code>
     * @param listener
     * @param result
     */
    void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result);

  }

  /**
   * Based on rpc type, work is either performed synch or asynchronously.
   * If work is done asynchronously, assemble the dependencies and hand them off to the worker via submitWork.
   */
  static class WorkIngestorImpl implements WorkIngestor {

    private final Provider<UserWorker> worker;

    WorkIngestorImpl(Provider<UserWorker> worker) {
      this.worker = worker;
    }

    private <M extends Message> M parse(byte[] body, Parser<M> parser, Class<M> type) throws RpcException {

      try {
        return parser.parseFrom(body);
      } catch (InvalidProtocolBufferException e) {
        throw  new RpcException(String.format("Failure while decoding %s body.", type.getSimpleName()), e);
      }
    }

    @Override
    public void feedWork(UserClientConnectionImpl connection, int rpcType, byte[] pBody, ByteBuf dBody,
                         ResponseSender responseSender) throws  RpcException {
    final UserWorker worker = this.worker.get();
    final TerminationListenerRegistry registry = connection;
    switch (rpcType) {

      case RpcType.RUN_QUERY_VALUE: {
        logger.debug("Received query to run.  Returning query handle.");
        final RunQuery query = parse(pBody, RunQuery.PARSER, RunQuery.class);
        UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
        final ExternalId externalId = ExternalIdHelper.generateExternalId();
        worker.submitWork(externalId, connection.getSession(), new UserConnectionResponseHandler(connection), request, registry);
        responseSender.send(new Response(RpcType.QUERY_HANDLE, ExternalIdHelper.toQueryId(externalId)));
        break;
      }

      case RpcType.CANCEL_QUERY_VALUE: {
        final QueryId queryId = parse(pBody, QueryId.PARSER, QueryId.class);
        final Ack ack = worker.cancelQuery(ExternalIdHelper.toExternal(queryId),
          connection.getSession().getCredentials().getUserName());
        responseSender.send(new Response(RpcType.ACK, ack));
        break;
      }

      case RpcType.RESUME_PAUSED_QUERY_VALUE: {
        final QueryId queryId = parse(pBody, QueryId.PARSER, QueryId.class);
        final Ack ack = worker.resumeQuery(ExternalIdHelper.toExternal(queryId));
        responseSender.send(new Response(RpcType.ACK, ack));
        break;
      }

      case RpcType.GET_CATALOGS_VALUE: {
        final GetCatalogsReq req = parse(pBody, GetCatalogsReq.PARSER, GetCatalogsReq.class);
        UserRequest request = new UserRequest(RpcType.GET_CATALOGS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.CatalogsHandler(responseSender), request, registry);
        break;
      }

      case RpcType.GET_SCHEMAS_VALUE: {
        final GetSchemasReq req = parse(pBody, GetSchemasReq.PARSER, GetSchemasReq.class);
        UserRequest request = new UserRequest(RpcType.GET_SCHEMAS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.SchemasHandler(responseSender), request, registry);
        break;
      }

      case RpcType.GET_TABLES_VALUE: {
        final GetTablesReq req = parse(pBody, GetTablesReq.PARSER, GetTablesReq.class);
        UserRequest request = new UserRequest(RpcType.GET_TABLES, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.TablesHandler(responseSender), request, registry);
        break;
      }

      case RpcType.GET_COLUMNS_VALUE: {
        final GetColumnsReq req = parse(pBody, GetColumnsReq.PARSER, GetColumnsReq.class);
        UserRequest request = new UserRequest(RpcType.GET_COLUMNS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.ColumnsHandler(responseSender), request, registry);
        break;
      }

      case RpcType.CREATE_PREPARED_STATEMENT_VALUE: {
        final CreatePreparedStatementReq req = parse(pBody, CreatePreparedStatementReq.PARSER, CreatePreparedStatementReq.class);
        UserRequest request = new UserRequest(RpcType.CREATE_PREPARED_STATEMENT, req);
        worker.submitWork(connection.getSession(), new PreparedStatementProvider.PreparedStatementHandler(responseSender), request, registry);
        break;
      }

      case RpcType.CREATE_PREPARED_STATEMENT_ARROW_VALUE: {
        final CreatePreparedStatementArrowReq req = parse(pBody, CreatePreparedStatementArrowReq.PARSER, CreatePreparedStatementArrowReq.class);
        UserRequest request = new UserRequest(RpcType.CREATE_PREPARED_STATEMENT_ARROW, req);
        worker.submitWork(connection.getSession(), new PreparedStatementProvider.PreparedStatementArrowHandler(responseSender), request, registry);
        break;
      }

      case RpcType.GET_SERVER_META_VALUE: {
        final GetServerMetaReq req = parse(pBody, GetServerMetaReq.PARSER, GetServerMetaReq.class);
        UserRequest request = new UserRequest(RpcType.GET_SERVER_META, req);
        worker.submitWork(connection.getSession(), new ServerMetaProvider.ServerMetaHandler(responseSender), request, registry);
        break;
      }

      default:
        throw new UnsupportedOperationException(String.format("UserServer received rpc of unknown type.  Type was %d.", rpcType));
    }
  }

  }

  private final static RpcEndpointInfos UNKNOWN = RpcEndpointInfos.newBuilder().build();

  /**
   * {@link RemoteConnection} implementation for user connection. Also implements {@link UserClientConnection}.
   */
  public class UserClientConnectionImpl extends RemoteConnection implements UserClientConnection {
    private final UUID uuid;
    private InetSocketAddress remote;
    private UserSession session;
    private OptionManager optionManager;

    public UserClientConnectionImpl(SocketChannel channel) {
      super(channel, "user client", false);
      uuid = UUID.randomUUID();
      remote = channel.remoteAddress();
    }

    void disableReadTimeout() {
      getChannel().pipeline().remove(TIMEOUT_HANDLER);
    }

    UUID getUuid() {
      return uuid;
    }

    void setUser(final UserToBitHandshake inbound) throws IOException {
      // First thing: log connection attempt
      final RpcEndpointInfos info = inbound.hasClientInfos() ? inbound.getClientInfos() : UNKNOWN;
      String infoMsg = String.format("[%s] Connection opened.\n" +
          "\tEndpoint: %s:%d\n" +
          "\tProtocol Version: %d\n" +
          "\tRecord Type: %s\n" +
          "\tRecord Formats: %s\n" +
          "\tSupport Complex Types: %s\n" +
          "\tName: %s\n" +
          "\tVersion: %s (%d.%d.%d)\n" +
          "\tApplication: %s\n",
          uuid.toString(), remote.getHostString(), remote.getPort(), inbound.getRpcVersion(),
          inbound.hasRecordBatchType() ? inbound.getRecordBatchType().name() : "n/a",
          Joiner.on(", ").join(inbound.getSupportedRecordBatchFormatsList()),
          inbound.hasSupportComplexTypes() ? inbound.getSupportComplexTypes() : "n/a",
          info.hasName() ? info.getName() : "n/a",
          info.hasVersion() ? info.getVersion() : "n/a",
          info.hasMajorVersion() ? info.getMajorVersion() : 0,
          info.hasMinorVersion() ? info.getMinorVersion() : 0,
          info.hasPatchVersion() ? info.getPatchVersion() : 0,
          info.hasApplication() ? info.getApplication() : "n/a"
          );

      if(!inbound.hasProperties() || inbound.getProperties().getPropertiesCount() == 0){
        infoMsg += "\tUser Properties: none";
      } else {
        infoMsg += "\tUser Properties: \n";
        UserProperties props = inbound.getProperties();
        for (int i = 0; i < props.getPropertiesCount(); i++) {
          Property p = props.getProperties(i);
          final String key = p.getKey().toLowerCase();
          if (key.contains("password") || key.contains("pwd")) {
            continue;
          }
          infoMsg += "\t\t" + p.getKey() + "=" + p.getValue() + "\n";
        }
      }

      CONNECTION_LOGGER.info(infoMsg);

      UserSession.Builder builder = UserSession.Builder.newBuilder();

      // Support legacy drill driver. If the client info is empty or it exists
      // and doesn't contain dremio, assume it is Apache Drill format.
      boolean legacyDriver = false;
      if (inbound.hasClientInfos()) {
        RpcEndpointInfos endpointInfos = inbound.getClientInfos();
        builder.withClientInfos(endpointInfos);
        if (!endpointInfos.hasName() ||
            !endpointInfos.getName().toLowerCase().contains("dremio")) {
          legacyDriver = true;
        }
      } else {
        legacyDriver = true;
      }

      // Set default catalog and quoting based on legacy vs dremio
      if (legacyDriver) {
        builder.withLegacyCatalog()
            .withInitialQuoting(Quoting.BACK_TICK)
            // backward compatibility is only supported for scalar types
            .setSupportComplexTypes(false);
      }

      SessionOptionManager sessionOptionManager = sessionOptionManagerFactory.getOrCreate(uuid.toString());
      builder
          .withCredentials(inbound.getCredentials())
          .withSessionOptionManager(sessionOptionManager, UserRPCServer.this.worker.get().getSystemOptions())
          .withUserProperties(inbound.getProperties())
          .setSupportComplexTypes(inbound.getSupportComplexTypes());

      // Choose batch record format to use
      if (inbound.hasRecordBatchType()) {
        switch (inbound.getRecordBatchType()) {
        case DRILL:
          // Only Drill 1.0 format supported for Dremio ODBC/Drill clients.
          builder.withRecordBatchFormat(RecordBatchFormat.DRILL_1_0);
          break;

        case DREMIO:
          try {
            builder.withRecordBatchFormat(chooseDremioRecordBatchFormat(inbound));
          } catch(IllegalArgumentException e) {
            throw new UserRpcException(nodeEndpointProvider.get(), e.getMessage(), e);
          }
          break;
        }
      } else {
        // Use legacy format
        builder = builder.withRecordBatchFormat(RecordBatchFormat.DRILL_1_0);
      }

      this.session = builder.build();

      try {
        if (session.useLegacyCatalogName()) {
          // set this to a system option so remote reads of information schema also know about this.
          session.getOptions().setOption(OptionValue.createBoolean(OptionType.SESSION, ExecConstants.USE_LEGACY_CATALOG_NAME.getOptionName(), true));
        }

        final String targetName = session.getTargetUserName();
        if (impersonationManager != null && targetName != null) {
          impersonationManager.replaceUserOnSession(targetName, session);
        }
      } catch (Exception e) {
        sessionOptionManagerFactory.delete(uuid.toString());
        throw e;
      }
    }

    @Override
    public UserSession getSession(){
      return session;
    }

    @Override
    public void sendResult(final RpcOutcomeListener<Ack> listener, final QueryResult result) {
      logger.trace("Sending result to client with {}", result);
      send(listener, this, RpcType.QUERY_RESULT, result, Ack.class, true);
    }

    @Override
    public void sendData(final RpcOutcomeListener<Ack> listener, final QueryWritableBatch result) {
      logger.trace("Sending data to client with {}", result);
      send(listener, this, RpcType.QUERY_DATA, result.getHeader(), Ack.class, false, result.getBuffers());
    }

    @Override
    public BufferAllocator getAllocator() {
      return allocator;
    }

    @Override
    public void addTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
      getChannel().closeFuture().addListener(listener);
    }

    @Override
    public void removeTerminationListener(GenericFutureListener<? extends Future<? super Void>> listener) {
      getChannel().closeFuture().removeListener(listener);
    }

  }

  @Override
  public UserClientConnectionImpl initRemoteConnection(SocketChannel channel) {
    return new UserClientConnectionImpl(channel);
  }

  @Override
  protected ChannelFutureListener newCloseListener(SocketChannel channel,
                                                   final UserClientConnectionImpl connection) {
    final ChannelFutureListener delegate = super.newCloseListener(channel, connection);

    return new ChannelFutureListener(){

      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        try {
          delegate.operationComplete(future);
          if (connection != null) {
            sessionOptionManagerFactory.delete(connection.uuid.toString());
          }
        } finally {
            CONNECTION_LOGGER.info("[{}] Connection Closed", connection.uuid);
        }
      }};
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> newHandshakeHandler(final UserClientConnectionImpl connection) {

    return new ServerHandshakeHandler<UserToBitHandshake>(RpcType.HANDSHAKE, UserToBitHandshake.PARSER){

      @Override
      protected void consumeHandshake(ChannelHandlerContext ctx, UserToBitHandshake inbound) throws Exception {
        BitToUserHandshake handshakeResp = getHandshakeResponse(inbound);
        OutboundRpcMessage msg = new OutboundRpcMessage(RpcMode.RESPONSE, this.handshakeType, coordinationId, handshakeResp);
        ctx.writeAndFlush(msg);

        if (handshakeResp.getStatus() != HandshakeStatus.SUCCESS) {
          // If handling handshake results in an error, throw an exception to terminate the connection.
          throw new RpcException("Handshake request failed: " + handshakeResp.getErrorMessage());
        }
      }

      @Override
      public BitToUserHandshake getHandshakeResponse(UserToBitHandshake inbound) throws Exception {
        logger.trace("Handling handshake from user to bit. {}", inbound);


        // if timeout is unsupported or is set to false, disable timeout.
        if (!inbound.hasSupportTimeout() || !inbound.getSupportTimeout()) {
          connection.disableReadTimeout();
          logger.warn("Timeout Disabled as client doesn't support it.", connection.getName());
        }

        final BitToUserHandshake.Builder respBuilder = BitToUserHandshake.newBuilder()
            .setRpcVersion(UserRpcConfig.RPC_VERSION)
            .setServerInfos(UserRpcUtils.getRpcEndpointInfos("Dremio Server"))
            .addAllSupportedMethods(UserRpcConfig.SUPPORTED_SERVER_METHODS);

        try {
          if (inbound.getRpcVersion() != UserRpcConfig.RPC_VERSION) {
            final String errMsg = String.format("Invalid rpc version. Expected %d, actual %d.",
                UserRpcConfig.RPC_VERSION, inbound.getRpcVersion());

            return handleFailure(respBuilder, HandshakeStatus.RPC_VERSION_MISMATCH, errMsg, null);
          }

          UserService userService = userServiceProvider.get();
          if (userService != null) {
            try {
              String password = "";
              final UserProperties props = inbound.getProperties();
              for (int i = 0; i < props.getPropertiesCount(); i++) {
                Property prop = props.getProperties(i);
                if (UserSession.PASSWORD.equalsIgnoreCase(prop.getKey())) {
                  password = prop.getValue();
                  break;
                }
              }
              userService.authenticate(inbound.getCredentials().getUserName(), password);
            } catch (UserLoginException ex) {
              return handleFailure(respBuilder, HandshakeStatus.AUTH_FAILED, ex.getMessage(), ex);
            }
          }

          connection.setUser(inbound);

          final RecordBatchFormat recordBatchFormat = connection.session.getRecordBatchFormat();
          respBuilder.setRecordBatchFormat(recordBatchFormat);

          // Note that the allocators created below are used for backward compatibility encoding. The encoder is
          // responsible for closing the allocator during its removal; this ensures any outstanding buffers in
          // Netty pipeline are released BEFORE the allocator is closed.
          // See BackwardsCompatibilityEncoder#handlerRemoved
          switch (recordBatchFormat) {
          case DRILL_1_0: {
            final BufferAllocator bcAllocator = allocator.newChildAllocator(connection.uuid.toString()
                + "-backward-compatibility-allocator", 0, Long.MAX_VALUE);
            logger.debug("Adding backwards compatibility encoder");
            connection.getChannel()
            .pipeline()
            .addAfter(PROTOCOL_ENCODER, DRILL_COMPATIBILITY_ENCODER,
                new BackwardsCompatibilityEncoder(new DrillBackwardsCompatibilityHandler(bcAllocator)));
          }
          break;

          case DREMIO_0_9: {
            /*
             * From Dremio 1.4 onwards we have moved to Little Endian Decimal format. We need to
             * add a new encoder in the netty pipeline when talking to old (1.3 and less) Dremio
             * Jdbc drivers.
             */
            final BufferAllocator bcAllocator = allocator.newChildAllocator(connection.uuid.toString()
                + "-dremio09-backward", 0, Long.MAX_VALUE);
            logger.debug("Adding dremio 09 backwards compatibility encoder");
            connection.getChannel()
            .pipeline()
            .addAfter(PROTOCOL_ENCODER, UserRpcUtils.DREMIO09_COMPATIBILITY_ENCODER,
                new BackwardsCompatibilityEncoder(new Dremio09BackwardCompatibilityHandler(bcAllocator)));
          }
          break;

          case DREMIO_1_4:
          default:

          }
          return respBuilder
              .setStatus(HandshakeStatus.SUCCESS)
              .build();
        } catch (Exception e) {
          return handleFailure(respBuilder, HandshakeStatus.UNKNOWN_FAILURE, e.getMessage(), e);
        }
      }
    };
  }

  /**
   * Complete building the given builder for <i>BitToUserHandshake</i> message with given status and error details.
   *
   * @param respBuilder Instance of {@link BitToUserHandshake} builder which
   *                    has RPC version field already set.
   * @param status  Status of handling handshake request.
   * @param errMsg  Error message.
   * @param exception Optional exception.
   * @return
   */
  private static BitToUserHandshake handleFailure(BitToUserHandshake.Builder respBuilder, HandshakeStatus status,
      String errMsg, Exception exception) {
    final String errorId = UUID.randomUUID().toString();

    if (exception != null) {
      logger.error("Error {} in Handling handshake request: {}, {}", errorId, status, errMsg, exception);
    } else {
      logger.error("Error {} in Handling handshake request: {}, {}", errorId, status, errMsg);
    }

    return respBuilder
        .setStatus(status)
        .setErrorId(errorId)
        .setErrorMessage(errMsg)
        .build();
  }

  @Override
  protected MessageDecoder newDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator);
  }

}
