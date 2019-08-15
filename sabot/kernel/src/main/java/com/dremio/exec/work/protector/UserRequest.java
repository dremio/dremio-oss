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
package com.dremio.exec.work.protector;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.ExecProtos.ServerPreparedStatementState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.exec.proto.UserBitShared.WorkloadType;
import com.dremio.exec.proto.UserProtos.CreatePreparedStatementReq;
import com.dremio.exec.proto.UserProtos.GetCatalogsReq;
import com.dremio.exec.proto.UserProtos.GetColumnsReq;
import com.dremio.exec.proto.UserProtos.GetSchemasReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetTablesReq;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.RunQuery;
import com.dremio.proto.model.attempts.RequestType;
import com.google.common.base.Preconditions;

/**
 * A wrapping class that can support two key types of jobs:
 *   - a basic sql query that return results via a number of push messages
 *   - a command response, that returns results via an RPC response.
 */
public class UserRequest {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserRequest.class);

  private static final long MAX_MEMORY_UTIL = 1_000_000;

  private static final QueryPriority NRT = QueryPriority.newBuilder()
    .setWorkloadClass(WorkloadClass.NRT)
    .setWorkloadType(WorkloadType.DDL).build();
  private static final QueryPriority GENERAL = QueryPriority.newBuilder().setWorkloadClass(WorkloadClass.GENERAL).build();

  private final RpcType type;
  private final Object request;
  /** if set to true, query is not going to be scheduled on a separate thread */
  private final boolean runInSameThread;

  public UserRequest(RpcType type, Object request) {
    this(type, request, false);
  }

  public UserRequest(RpcType type, Object request, boolean runInSameThread) {
    this.type = Preconditions.checkNotNull(type);
    this.request = Preconditions.checkNotNull(request);
    this.runInSameThread = runInSameThread;
  }

  public RpcType getType() {
    return type;
  }

  public boolean runInSameThread() {
    return runInSameThread;
  }

  public <X> X unwrap(Class<X> clazz) {
    Preconditions.checkArgument(clazz.isAssignableFrom(request.getClass()),
      "Unable to cast request type to class %s", clazz.getName());
    return clazz.cast(request);
  }

  public QueryPriority getPriority(){
    switch(type){
    case CREATE_PREPARED_STATEMENT:
    case GET_CATALOGS:
    case GET_COLUMNS:
    case GET_QUERY_PLAN_FRAGMENTS:
    case GET_SCHEMAS:
    case GET_TABLES:
    case GET_SERVER_META:
      return NRT;

    case RUN_QUERY:
      RunQuery q = unwrap(RunQuery.class);
      if(q.hasPriority()){
        return q.getPriority();
      }
      return GENERAL;
    default:
      throw new IllegalStateException("Invalid type: " + type);

    }
  }

  public long getMaxAllocation(){
    switch(type){
    case CREATE_PREPARED_STATEMENT:
    case GET_CATALOGS:
    case GET_COLUMNS:
    case GET_SCHEMAS:
    case GET_TABLES:
    case GET_SERVER_META:
      return MAX_MEMORY_UTIL;
    case RUN_QUERY:
      RunQuery q = unwrap(RunQuery.class);
      if(q.hasMaxAllocation()){
        return q.getMaxAllocation();
      }
      return Long.MAX_VALUE;
    default:
      throw new IllegalStateException("Invalid type: " + type);
    }
  }

  public String getDescription() {
    switch(type){
    case CREATE_PREPARED_STATEMENT:
      return "[Prepare Statement] " + unwrap(CreatePreparedStatementReq.class).getSqlQuery();

    case GET_CATALOGS: {
      GetCatalogsReq req = unwrap(GetCatalogsReq.class);
      return String.format("[Get Catalogs] Catalog Filter: %s.", req.getCatalogNameFilter());
    }

    case GET_COLUMNS: {
      GetColumnsReq req = unwrap(GetColumnsReq.class);
      return String.format("[Get Columns] Catalog Filter: %s, Schema Filter: %s, Table Filter %s, Column Filter %s.",
        req.getCatalogNameFilter(), req.getSchemaNameFilter(), req.getTableNameFilter(), req.getColumnNameFilter());
    }

    case GET_SCHEMAS: {
      GetSchemasReq req = unwrap(GetSchemasReq.class);
      return String.format("[Get Schemas] Catalog Filter: %s, Schema Filter: %s.",
        req.getCatalogNameFilter(), req.getSchemaNameFilter());
    }

    case GET_TABLES: {
      GetTablesReq req = unwrap(GetTablesReq.class);
      return String.format("[Get Tables] Catalog Filter: %s, Schema Filter: %s, Table Filter %s.",
        req.getCatalogNameFilter(), req.getSchemaNameFilter(), req.getTableNameFilter());
    }

    case GET_SERVER_META: {
      GetServerMetaReq req = unwrap(GetServerMetaReq.class);
      return String.format("[Get Server Meta]");
    }

    case RUN_QUERY:
      RunQuery q = unwrap(RunQuery.class);
      if(q.hasDescription()){
        return q.getDescription();
      } else {
        return getSql(q);
      }
    default:
      throw new IllegalStateException("Invalid type: " + type);
    }
  }

  private static String getSql(final RunQuery q) {
    if(q.getType() == QueryType.PREPARED_STATEMENT){
      try{
        final ServerPreparedStatementState preparedStatement =
          ServerPreparedStatementState.PARSER.parseFrom(q.getPreparedStatementHandle().getServerInfo());
        return preparedStatement.getSqlQuery();
      }catch(Exception ex){
        throw UserException.connectionError(ex).message("Prepared statement provided is corrupt.").build(logger);
      }
    } else {
      return q.getPlan();
    }
  }

  public String getSql() {
    switch(type){
    case CREATE_PREPARED_STATEMENT:
      return unwrap(CreatePreparedStatementReq.class).getSqlQuery();
    case  RUN_QUERY:
      return getSql(unwrap(RunQuery.class));
    default:
      return "NA";
    }
  }

  public RequestType getRequestType() {
    switch (type) {
    case CREATE_PREPARED_STATEMENT:
      return RequestType.CREATE_PREPARE;
    case GET_CATALOGS:
      return RequestType.GET_CATALOGS;
    case GET_COLUMNS:
      return RequestType.GET_COLUMNS;
    case GET_SCHEMAS:
      return RequestType.GET_SCHEMAS;
    case GET_TABLES:
      return RequestType.GET_TABLES;
    case GET_SERVER_META:
      return RequestType.GET_SERVER_META;
    case RUN_QUERY:
      RunQuery q = unwrap(RunQuery.class);
      if (q.getType() == QueryType.PREPARED_STATEMENT) {
        return RequestType.EXECUTE_PREPARE;
      } else {
        return RequestType.RUN_SQL;
      }
    default:
      throw new IllegalStateException("Invalid type: " + type);
    }
  }

}
