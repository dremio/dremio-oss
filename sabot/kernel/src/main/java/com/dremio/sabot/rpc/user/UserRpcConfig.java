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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.GeneralRPCProtos.Ack;
import com.dremio.exec.proto.UserBitShared.QueryData;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.exec.proto.UserProtos.BitToUserHandshake;
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
import com.dremio.exec.proto.UserProtos.QueryPlanFragments;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.exec.proto.UserProtos.UserToBitHandshake;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class UserRpcConfig {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserRpcConfig.class);

  public static RpcConfig getMapping(SabotConfig config, Executor executor, Optional<SSLConfig> sslConfig) {
    return RpcConfig.newBuilder()
        .name("USER")
        .timeout(config.getInt(ExecConstants.USER_RPC_TIMEOUT))
        .executor(executor)
        .sslConfig(sslConfig)
        .add(RpcType.HANDSHAKE, UserToBitHandshake.class, RpcType.HANDSHAKE, BitToUserHandshake.class) // user to bit
        .add(RpcType.RUN_QUERY, RunQuery.class, RpcType.QUERY_HANDLE, QueryId.class) // user to bit
        .add(RpcType.CANCEL_QUERY, QueryId.class, RpcType.ACK, Ack.class) // user to bit
        .add(RpcType.QUERY_DATA, QueryData.class, RpcType.ACK, Ack.class) // bit to user
        .add(RpcType.QUERY_RESULT, QueryResult.class, RpcType.ACK, Ack.class) // bit to user
        .add(RpcType.RESUME_PAUSED_QUERY, QueryId.class, RpcType.ACK, Ack.class) // user to bit
        .add(RpcType.GET_QUERY_PLAN_FRAGMENTS, GetQueryPlanFragments.class,
          RpcType.QUERY_PLAN_FRAGMENTS, QueryPlanFragments.class) // user to bit
        .add(RpcType.GET_CATALOGS, GetCatalogsReq.class, RpcType.CATALOGS, GetCatalogsResp.class) // user to bit
        .add(RpcType.GET_SCHEMAS, GetSchemasReq.class, RpcType.SCHEMAS, GetSchemasResp.class) // user to bit
        .add(RpcType.GET_TABLES, GetTablesReq.class, RpcType.TABLES, GetTablesResp.class) // user to bit
        .add(RpcType.GET_COLUMNS, GetColumnsReq.class, RpcType.COLUMNS, GetColumnsResp.class) // user to bit
        .add(RpcType.CREATE_PREPARED_STATEMENT, CreatePreparedStatementReq.class,
            RpcType.PREPARED_STATEMENT, CreatePreparedStatementResp.class) // user to bit
        .add(RpcType.GET_SERVER_META, GetServerMetaReq.class, RpcType.SERVER_META, GetServerMetaResp.class) // user to bit
        .build();
  }

  public static int RPC_VERSION = 5;

  /**
   * Contains the list of methods supported by the server (from user to bit)
   */
  public static final Set<RpcType> SUPPORTED_SERVER_METHODS = Sets.immutableEnumSet(
      ImmutableSet
        .<RpcType> builder()
        .add(RpcType.RUN_QUERY, RpcType.CANCEL_QUERY, RpcType.GET_QUERY_PLAN_FRAGMENTS, RpcType.RESUME_PAUSED_QUERY,
            RpcType.GET_CATALOGS, RpcType.GET_SCHEMAS, RpcType.GET_TABLES, RpcType.GET_COLUMNS,
            RpcType.CREATE_PREPARED_STATEMENT, RpcType.GET_SERVER_META)
        .build()
      );
}
