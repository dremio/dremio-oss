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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.Executor;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserBitShared.RpcEndpointInfos;
import com.dremio.exec.proto.UserProtos.BitToUserHandshake;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementResp;
import com.dremio.exec.proto.UserProtos.GetCatalogsResp;
import com.dremio.exec.proto.UserProtos.GetColumnsResp;
import com.dremio.exec.proto.UserProtos.GetQueryPlanFragments;
import com.dremio.exec.proto.UserProtos.GetSchemasResp;
import com.dremio.exec.proto.UserProtos.GetServerMetaResp;
import com.dremio.exec.proto.UserProtos.GetTablesResp;
import com.dremio.exec.proto.UserProtos.HandshakeStatus;
import com.dremio.exec.proto.UserProtos.QueryPlanFragments;
import com.dremio.exec.proto.UserProtos.RecordBatchFormat;
import com.dremio.exec.proto.UserProtos.RecordBatchType;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.UserProperties;
import com.dremio.exec.proto.UserProtos.UserToBitHandshake;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.BasicClient;
import com.dremio.exec.rpc.BasicClientWithConnection;
import com.dremio.exec.rpc.ConnectionThrottle;
import com.dremio.exec.rpc.MessageDecoder;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.RpcConnectionHandler;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.rpc.RpcFuture;
import com.google.common.collect.Sets;
import com.google.protobuf.MessageLite;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

public class UserClient extends BasicClientWithConnection<RpcType, UserToBitHandshake, BitToUserHandshake> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserClient.class);

  private final QueryResultHandler queryResultHandler = new QueryResultHandler();
  private final boolean supportComplexTypes;

  private final String clientName;
  private volatile RpcEndpointInfos serverInfos = null;
  private volatile Set<RpcType> supportedMethods = null;

  public UserClient(String clientName, SabotConfig config, boolean supportComplexTypes, BufferAllocator alloc,
      EventLoopGroup eventLoopGroup, Executor eventExecutor) {
    super(
        UserRpcConfig.getMapping(config, eventExecutor),
        alloc,
        eventLoopGroup,
        RpcType.HANDSHAKE,
        BitToUserHandshake.class,
        BitToUserHandshake.PARSER,
        "user client");
    this.clientName = checkNotNull(clientName);
    this.supportComplexTypes = supportComplexTypes;
  }

  public RpcEndpointInfos getServerInfos() {
    return serverInfos;
  }

  public Set<RpcType> getSupportedMethods() {
    return supportedMethods;
  }

  public void submitQuery(UserResultsListener resultsListener, RunQuery query) {
    send(queryResultHandler.getWrappedListener(resultsListener), RpcType.RUN_QUERY, query, QueryId.class);
  }

  public void connect(RpcConnectionHandler<ServerConnection> handler, NodeEndpoint endpoint,
                      UserProperties props, UserBitShared.UserCredentials credentials) {
    UserToBitHandshake.Builder hsBuilder = UserToBitHandshake.newBuilder()
        .setRpcVersion(UserRpcConfig.RPC_VERSION)
        .setSupportListening(true)
        .setSupportComplexTypes(supportComplexTypes)
        .setSupportTimeout(true)
        .setCredentials(credentials)
        .setRecordBatchType(RecordBatchType.DREMIO)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_1_4)
        .addSupportedRecordBatchFormats(RecordBatchFormat.DREMIO_0_9)
        .setClientInfos(UserRpcUtils.getRpcEndpointInfos(clientName));
    if (props != null) {
      hsBuilder.setProperties(props);
    }

    this.connectAsClient(queryResultHandler.getWrappedConnectionHandler(handler),
        hsBuilder.build(), endpoint.getAddress(), endpoint.getUserPort());
  }

  @Override
  protected MessageLite getResponseDefaultInstance(int rpcType) throws RpcException {
    switch (rpcType) {
    case RpcType.ACK_VALUE:
      return Ack.getDefaultInstance();
    case RpcType.HANDSHAKE_VALUE:
      return BitToUserHandshake.getDefaultInstance();
    case RpcType.QUERY_HANDLE_VALUE:
      return QueryId.getDefaultInstance();
    case RpcType.QUERY_RESULT_VALUE:
      return QueryResult.getDefaultInstance();
    case RpcType.QUERY_DATA_VALUE:
      return QueryData.getDefaultInstance();
    case RpcType.QUERY_PLAN_FRAGMENTS_VALUE:
      return QueryPlanFragments.getDefaultInstance();
    case RpcType.CATALOGS_VALUE:
      return GetCatalogsResp.getDefaultInstance();
    case RpcType.SCHEMAS_VALUE:
      return GetSchemasResp.getDefaultInstance();
    case RpcType.TABLES_VALUE:
      return GetTablesResp.getDefaultInstance();
    case RpcType.COLUMNS_VALUE:
      return GetColumnsResp.getDefaultInstance();
    case RpcType.PREPARED_STATEMENT_VALUE:
      return CreatePreparedStatementResp.getDefaultInstance();
    case RpcType.SERVER_META_VALUE:
      return GetServerMetaResp.getDefaultInstance();
    }
    throw new RpcException(String.format("Unable to deal with RpcType of %d", rpcType));
  }

  @Override
  protected Response handleReponse(ConnectionThrottle throttle, int rpcType, byte[] pBody, ByteBuf dBody) throws RpcException {
    switch (rpcType) {
    case RpcType.QUERY_DATA_VALUE:
      queryResultHandler.batchArrived(throttle, pBody, dBody);
      return new Response(RpcType.ACK, Acks.OK);
    case RpcType.QUERY_RESULT_VALUE:
      queryResultHandler.resultArrived(pBody);
      return new Response(RpcType.ACK, Acks.OK);
    default:
      throw new RpcException(String.format("Unknown Rpc Type %d. ", rpcType));
    }
  }

  @Override
  protected void validateHandshake(BitToUserHandshake inbound) throws RpcException {
//    logger.debug("Handling handshake from bit to user. {}", inbound);
    if (inbound.getStatus() != HandshakeStatus.SUCCESS) {
      final String errMsg = String.format("Status: %s, Error Id: %s, Error message: %s",
          inbound.getStatus(), inbound.getErrorId(), inbound.getErrorMessage());
      logger.error(errMsg);
      throw new RpcException(errMsg);
    }

    // Successful connection...
    if (inbound.hasServerInfos()) {
      serverInfos = inbound.getServerInfos();
    }
    supportedMethods = Sets.immutableEnumSet(inbound.getSupportedMethodsList());
    // Older servers don't return record batch format: assume pre-1.4 servers
    RecordBatchFormat recordBatchFormat = inbound.hasRecordBatchFormat() ? inbound.getRecordBatchFormat() : RecordBatchFormat.DREMIO_0_9;

    switch(recordBatchFormat) {
    case DREMIO_1_4:
      break;

    case DREMIO_0_9:
    {
      /*
       * From Dremio 1.4 onwards we have moved to Little Endian Decimal format. We need to
       * add a new decoder in the netty pipeline when talking to old (1.3 and less) Dremio
       * servers.
       */
      final BufferAllocator bcAllocator = connection.getAllocator()
          .newChildAllocator("dremio09-backward", 0, Long.MAX_VALUE);
      logger.debug("Adding dremio 09 backwards compatibility decoder");
      connection.getChannel()
        .pipeline()
        .addAfter(BasicClient.PROTOCOL_DECODER, "dremio09-backward",
          new BackwardsCompatibilityDecoder(bcAllocator, new Dremio09BackwardCompatibilityHandler(bcAllocator)));
      }
      break;

    case UNKNOWN:
    default:
      throw new RpcException("Unsupported record batch format: " + recordBatchFormat);
    }
  }

  @Override
  protected void finalizeConnection(BitToUserHandshake handshake, BasicClientWithConnection.ServerConnection connection) {
  }

  @Override
  public MessageDecoder getDecoder(BufferAllocator allocator) {
    return new UserProtobufLengthDecoder(allocator);
  }

  /**
   * planQuery is an API to plan a query without query execution
   * @param req - data necessary to plan query
   * @return list of PlanFragments that can later on be submitted for execution
   */
  public RpcFuture<QueryPlanFragments> planQuery(
      GetQueryPlanFragments req) {
    return send(RpcType.GET_QUERY_PLAN_FRAGMENTS, req, QueryPlanFragments.class);
  }
}
