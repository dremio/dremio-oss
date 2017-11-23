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
package com.dremio.sabot.rpc.user;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.avatica.util.Quoting;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.handlers.commands.MetadataProvider;
import com.dremio.exec.planner.sql.handlers.commands.PreparedStatementProvider;
import com.dremio.exec.planner.sql.handlers.commands.ServerMetaProvider;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.GeneralRPCProtos.RpcMode;
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
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcOutcomeListener;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValue;
import com.dremio.exec.server.options.OptionValue.OptionType;
import com.dremio.exec.work.ExternalIdHelper;
import com.dremio.exec.work.foreman.TerminationListenerRegistry;
import com.dremio.exec.work.protector.UserConnectionResponseHandler;
import com.dremio.exec.work.protector.UserRequest;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.sabot.op.screen.QueryWritableBatch;
import com.dremio.service.users.UserLoginException;
import com.dremio.service.users.UserService;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class UserRPCServer extends BasicServer<RpcType, UserRPCServer.UserClientConnectionImpl> {
  private static final org.slf4j.Logger CONNECTION_LOGGER = org.slf4j.LoggerFactory.getLogger("com.dremio.ConnectionLog");
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserRPCServer.class);

  private final Provider<UserWorker> worker;
  private final Provider<SabotContext> dbContext;
  private final InboundImpersonationManager impersonationManager;
  private final BufferAllocator allocator;

  UserRPCServer(
      BootStrapContext context,
      Provider<SabotContext> dbContext,
      Provider<UserWorker> worker,
      BufferAllocator allocator,
      EventLoopGroup eventLoopGroup,
      InboundImpersonationManager impersonationManager
      ) {
    super(UserRpcConfig.getMapping(context.getConfig(), context.getExecutor()),
        allocator.getAsByteBufAllocator(),
        eventLoopGroup);
    this.worker = worker;
    this.dbContext = dbContext;
    this.allocator = allocator;
    this.impersonationManager = impersonationManager;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    // configure the main pipeline
    super.initChannel(ch);
    ch.pipeline().addLast(new RpcCompatibilityEncoder());
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
  protected void handle(UserClientConnectionImpl connection, int rpcType, byte[] pBody, ByteBuf dBody,
      ResponseSender responseSender) throws RpcException {
    final UserWorker worker = this.worker.get();
    final TerminationListenerRegistry registry = connection;
    switch (rpcType) {

    case RpcType.RUN_QUERY_VALUE:
      logger.debug("Received query to run.  Returning query handle.");
      try {
        final RunQuery query = RunQuery.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.RUN_QUERY, query);
        final QueryId queryId = ExternalIdHelper.toQueryId(worker.submitWork(connection.getSession(), new UserConnectionResponseHandler(connection), request, registry));
        responseSender.send(new Response(RpcType.QUERY_HANDLE, queryId));
        break;
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding RunQuery body.", e);
      }

    case RpcType.CANCEL_QUERY_VALUE:
      try {
        final QueryId queryId = QueryId.PARSER.parseFrom(pBody);
        final Ack ack = worker.cancelQuery(ExternalIdHelper.toExternal(queryId));
        responseSender.send(new Response(RpcType.ACK, ack));
        break;
      } catch (InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding QueryId body.", e);
      }

    case RpcType.GET_CATALOGS_VALUE:
      try {
        final GetCatalogsReq req = GetCatalogsReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.GET_CATALOGS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.CatalogsHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetCatalogsReq body.", e);
      }
    case RpcType.GET_SCHEMAS_VALUE:
      try {
        final GetSchemasReq req = GetSchemasReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.GET_SCHEMAS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.SchemasHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetSchemasReq body.", e);
      }
    case RpcType.GET_TABLES_VALUE:
      try {
        final GetTablesReq req = GetTablesReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.GET_TABLES, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.TablesHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetTablesReq body.", e);
      }
    case RpcType.GET_COLUMNS_VALUE:
      try {
        final GetColumnsReq req = GetColumnsReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.GET_COLUMNS, req);
        worker.submitWork(connection.getSession(), new MetadataProvider.ColumnsHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding GetColumnsReq body.", e);
      }
    case RpcType.CREATE_PREPARED_STATEMENT_VALUE:
      try {
        final CreatePreparedStatementReq req =
            CreatePreparedStatementReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.CREATE_PREPARED_STATEMENT, req);
        worker.submitWork(connection.getSession(), new PreparedStatementProvider.PreparedStatementHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding CreatePreparedStatementReq body.", e);
      }
    case RpcType.GET_SERVER_META_VALUE:
      try {
        final GetServerMetaReq req =
            GetServerMetaReq.PARSER.parseFrom(pBody);
        UserRequest request = new UserRequest(RpcType.GET_SERVER_META, req);
        worker.submitWork(connection.getSession(), new ServerMetaProvider.ServerMetaHandler(responseSender), request, registry);
        break;
      } catch (final InvalidProtocolBufferException e) {
        throw new RpcException("Failure while decoding CreatePreparedStatementReq body.", e);
      }
    default:
      throw new UnsupportedOperationException(String.format("UserServer received rpc of unknown type.  Type was %d.", rpcType));
    }
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


  private final static RpcEndpointInfos UNKNOWN = RpcEndpointInfos.newBuilder().build();

  /**
   * {@link RemoteConnection} implementation for user connection. Also implements {@link UserClientConnection}.
   */
  public class UserClientConnectionImpl extends RemoteConnection implements UserClientConnection {
    private final UUID uuid;
    private InetSocketAddress remote;
    private UserSession session;

    public UserClientConnectionImpl(SocketChannel channel) {
      super(channel, "user client", false);
      uuid = UUID.randomUUID();
      remote = channel.remoteAddress();

    }

    void disableReadTimeout() {
      getChannel().pipeline().remove(BasicServer.TIMEOUT_HANDLER);
    }

    void setUser(final UserToBitHandshake inbound) throws IOException {
      final UserWorker worker = UserRPCServer.this.worker.get();
      UserSession.Builder builder = UserSession.Builder.newBuilder()
          .withCredentials(inbound.getCredentials())
          .withOptionManager(worker.getSystemOptions())
          .withUserProperties(inbound.getProperties())
          .setSupportComplexTypes(inbound.getSupportComplexTypes());

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
      if (legacyDriver) {
        builder.withLegacyCatalog()
            .withInitialQuoting(Quoting.BACK_TICK)
            // backward compatibility is only supported for scalar types
            .setSupportComplexTypes(false);
      }

      // check if we have quoting that we want to set.
      if(inbound.hasProperties()){
        UserProperties properties = inbound.getProperties();
        for(Property p : properties.getPropertiesList()){
          if(p.hasValue() && p.hasKey()) {
            if (p.getKey().equalsIgnoreCase("quoting")) {
              String value = p.getValue();
              if ("BACK_TICK".equalsIgnoreCase(value)) {
                builder.withInitialQuoting(Quoting.BACK_TICK);
              } else if ("DOUBLE_QUOTE".equalsIgnoreCase(value)) {
                builder.withInitialQuoting(Quoting.DOUBLE_QUOTE);
              } else if ("BRACKET".equalsIgnoreCase(value)) {
                builder.withInitialQuoting(Quoting.BRACKET);
              } else {
                logger.warn("Ignoring message to use initial quoting of type {}.", value);
              }
            } else if (p.getKey().equalsIgnoreCase("supportFullyQualifiedProjects")) {
              builder.withFullyQualifiedProjectsSupport("true".equalsIgnoreCase(p.getValue()));
            }
          }
        }
      }

      if (inbound.hasRecordBatchFormat()) {
        builder = builder.withRecordBatchFormat(inbound.getRecordBatchFormat());
      } else {
        // Use legacy format
        builder = builder.withRecordBatchFormat(RecordBatchFormat.DRILL_1_0);
      }
      this.session = builder.build();

      final RpcEndpointInfos info = inbound.hasClientInfos() ? inbound.getClientInfos() : UNKNOWN;
      String infoMsg = String.format("[%s] Connection opened.\n" +
          "\tEndpoint: %s:%d\n" +
          "\tProtocol Version: %d\n" +
          "\tRecord Format: %s\n" +
          "\tSupport Complex Types: %s\n" +
          "\tName: %s\n" +
          "\tVersion: %s (%d.%d.%d)\n" +
          "\tApplication: %s\n",
          uuid.toString(), remote.getHostString(), remote.getPort(), inbound.getRpcVersion(),
          inbound.hasRecordBatchFormat() ? inbound.getRecordBatchFormat().name() : "n/a",
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
        for(int i =0; i < props.getPropertiesCount(); i++){
          Property p = props.getProperties(i);
          if(p.getKey().equalsIgnoreCase("password")){
              continue;
          }
          infoMsg += "\t\t" + p.getKey() + "=" + p.getValue() + "\n";
        }
      }


      if(session.useLegacyCatalogName()){
        // set this to a system option so remote reads of information schema also know about this.
        session.getOptions().setOption(OptionValue.createBoolean(OptionType.SESSION, ExecConstants.USE_LEGACY_CATALOG_NAME.getOptionName(), true));
      }

      final String targetName = session.getTargetUserName();
      if (impersonationManager != null && targetName != null) {
        impersonationManager.replaceUserOnSession(targetName, session);
      }

      CONNECTION_LOGGER.info(infoMsg);
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
  protected ChannelFutureListener getCloseHandler(SocketChannel channel,
                                                  final UserClientConnectionImpl clientConnection) {
    final ChannelFutureListener delegate = super.getCloseHandler(channel, clientConnection);
    return new ChannelFutureListener(){

      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        try {
          delegate.operationComplete(future);
        } finally {
            CONNECTION_LOGGER.info("[{}] Connection Closed", clientConnection.uuid.toString());
        }
      }};
  }

  @Override
  protected ServerHandshakeHandler<UserToBitHandshake> getHandshakeHandler(final UserClientConnectionImpl connection) {

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

          if (dbContext.get().isUserAuthenticationEnabled()) {
            UserService userService = dbContext.get().getUserService();
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

          if(connection.session.getRecordBatchFormat() == RecordBatchFormat.DRILL_1_0) {
            // This allocator is used for backward compatibility encoding. Note that the encoder is responsible for
            // closing the allocator during it's removal; this ensures any outstanding buffers in Netty pipeline
            // are released BEFORE the allocator is closed. See BackwardsCompatibilityEncoder#handlerRemoved
            final BufferAllocator bcAllocator = allocator.newChildAllocator(connection.uuid.toString()
                    + "-backward-compatibility-allocator", 0, Long.MAX_VALUE);
            logger.debug("Adding backwards compatibility encoder");
            connection.getChannel()
                .pipeline()
                .addAfter(PROTOCOL_ENCODER, "backward-compatibility-encoder",
                    new BackwardsCompatibilityEncoder(bcAllocator));
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
   * @param respBuilder Instance of {@link com.dremio.exec.proto.UserProtos.BitToUserHandshake} builder which
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
  public MessageDecoder getDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator);
  }

}
