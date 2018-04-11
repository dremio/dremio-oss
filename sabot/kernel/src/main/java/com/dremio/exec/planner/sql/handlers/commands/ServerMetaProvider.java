/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.Arrays;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl.Metadata;
import org.apache.calcite.sql.parser.SqlParser;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.ParserConfig;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserProtos.ConvertSupport;
import com.dremio.exec.proto.UserProtos.CorrelationNamesSupport;
import com.dremio.exec.proto.UserProtos.DateTimeLiteralsSupport;
import com.dremio.exec.proto.UserProtos.GetServerMetaReq;
import com.dremio.exec.proto.UserProtos.GetServerMetaResp;
import com.dremio.exec.proto.UserProtos.GroupBySupport;
import com.dremio.exec.proto.UserProtos.IdentifierCasing;
import com.dremio.exec.proto.UserProtos.NullCollation;
import com.dremio.exec.proto.UserProtos.OrderBySupport;
import com.dremio.exec.proto.UserProtos.OuterJoinSupport;
import com.dremio.exec.proto.UserProtos.RecordBatchFormat;
import com.dremio.exec.proto.UserProtos.RequestStatus;
import com.dremio.exec.proto.UserProtos.RpcType;
import com.dremio.exec.proto.UserProtos.ServerMeta;
import com.dremio.exec.proto.UserProtos.SubQuerySupport;
import com.dremio.exec.proto.UserProtos.UnionSupport;
import com.dremio.exec.resolver.TypeCastRules;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.work.protector.ResponseSenderHandler;
import com.dremio.sabot.rpc.user.UserSession;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

/**
 * Contains worker {@link Runnable} for returning server meta information
 */
public class ServerMetaProvider {
  private static final ImmutableList<MinorType> DRILL10_DECIMAL_TYPES = ImmutableList.of(
      MinorType.DECIMAL9, MinorType.DECIMAL18,
      MinorType.DECIMAL28DENSE, MinorType.DECIMAL28SPARSE,
      MinorType.DECIMAL38DENSE, MinorType.DECIMAL38SPARSE);

  private static ServerMeta DEFAULT = ServerMeta.newBuilder()
      .addAllConvertSupport(getSupportedConvertOps())
      .addAllDateTimeFunctions(Splitter.on(",").split(SqlJdbcFunctionCall.getTimeDateFunctions()))
      .addAllDateTimeLiteralsSupport(Arrays.asList(DateTimeLiteralsSupport.values()))
      .addAllNumericFunctions(Splitter.on(",").split(SqlJdbcFunctionCall.getNumericFunctions()))
      .addAllOrderBySupport(Arrays.asList(OrderBySupport.OB_UNRELATED, OrderBySupport.OB_EXPRESSION))
      .addAllOuterJoinSupport(Arrays.asList(OuterJoinSupport.OJ_LEFT, OuterJoinSupport.OJ_RIGHT, OuterJoinSupport.OJ_FULL))
      .addAllStringFunctions(Splitter.on(",").split(SqlJdbcFunctionCall.getStringFunctions()))
      .addAllSystemFunctions(Splitter.on(",").split(SqlJdbcFunctionCall.getSystemFunctions()))
      .addAllSubquerySupport(Arrays.asList(SubQuerySupport.SQ_CORRELATED, SubQuerySupport.SQ_IN_COMPARISON, SubQuerySupport.SQ_IN_EXISTS, SubQuerySupport.SQ_IN_QUANTIFIED))
      .addAllUnionSupport(Arrays.asList(UnionSupport.U_UNION, UnionSupport.U_UNION_ALL))
      .setAllTablesSelectable(false)
      .setBlobIncludedInMaxRowSize(true)
      .setCatalogAtStart(true)
      .setCatalogSeparator(".")
      .setCatalogTerm("catalog")
      .setColumnAliasingSupported(true)
      .setNullPlusNonNullEqualsNull(true)
      .setCorrelationNamesSupport(CorrelationNamesSupport.CN_ANY)
      .setReadOnly(false)
      .setGroupBySupport(GroupBySupport.GB_UNRELATED)
      .setLikeEscapeClauseSupported(true)
      .setNullCollation(NullCollation.NC_HIGH)
      .setSchemaTerm("schema")
      .setSearchEscapeString("\\")
      .setTableTerm("table")
      .build();

  private static ServerMeta DRILL_1_0_DEFAULT = ServerMeta.newBuilder(DEFAULT)
      .clearConvertSupport()
      .addAllConvertSupport(getDrill10SupportedConvertOps())
      .build();


  private static final Iterable<ConvertSupport> getSupportedConvertOps() {
    // A set would be more appropriate but it's not possible to produce
    // duplicates, and an iterable is all we need.
    ImmutableList.Builder<ConvertSupport> supportedConvertedOps = ImmutableList.builder();

    for(MinorType from: MinorType.values()) {
      for(MinorType to: MinorType.values()) {
        if (TypeCastRules.isCastable(from, to)) {
          supportedConvertedOps.add(ConvertSupport.newBuilder().setFrom(from).setTo(to).build());
        }
      }
    }

    return supportedConvertedOps.build();
  }

  private static final Iterable<ConvertSupport> getDrill10SupportedConvertOps() {
    // A set would be more appropriate but it's not possible to produce
    // duplicates, and an iterable is all we need.
    ImmutableList.Builder<ConvertSupport> supportedConvertedOps = ImmutableList.builder();

    for(MinorType from: MinorType.values()) {
      for(MinorType to: MinorType.values()) {
        if (TypeCastRules.isCastable(from, to)) {
          if (from == MinorType.DECIMAL || to == MinorType.DECIMAL) {
            addDrill10DecimalConvertOps(supportedConvertedOps, from, to);
          }
          else {
            supportedConvertedOps.add(ConvertSupport.newBuilder().setFrom(from).setTo(to).build());
          }
        }
      }
    }

    return supportedConvertedOps.build();
  }



  private static final void addDrill10DecimalConvertOps(ImmutableList.Builder<ConvertSupport> builder, MinorType from, MinorType to) {
    ImmutableList.Builder<ConvertSupport> supportedConvertedOps = ImmutableList.builder();

    for(MinorType newFrom: from == MinorType.DECIMAL ? DRILL10_DECIMAL_TYPES : ImmutableList.of(from)) {
      for(MinorType newTo: to == MinorType.DECIMAL ? DRILL10_DECIMAL_TYPES : ImmutableList.of(to)) {
        supportedConvertedOps.add(ConvertSupport.newBuilder().setFrom(newFrom).setTo(newTo).build());
      }
    }
  }

  public static class ServerMetaHandler extends ResponseSenderHandler<GetServerMetaResp> {

    public ServerMetaHandler(ResponseSender sender) {
      super(RpcType.SERVER_META, GetServerMetaResp.class, sender);
    }

    @Override
    protected GetServerMetaResp getException(UserException ex) {
      final GetServerMetaResp.Builder respBuilder = GetServerMetaResp.newBuilder();
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(MetadataProvider.createPBError("get server meta", ex));
      return respBuilder.build();
    }

  }

  /**
   * Super class for all metadata provider runnable classes.
   */
  public static class ServerMetaCommandRunner implements CommandRunner<GetServerMetaResp> {
    @SuppressWarnings("unused")
    private final GetServerMetaReq req;
    private final QueryId queryId;
    private final UserSession session;
    private final SabotContext dContext;

    protected ServerMetaCommandRunner(
        final QueryId queryId,
        final UserSession session,
        final SabotContext dContext,
        final GetServerMetaReq req) {
      this.queryId = Preconditions.checkNotNull(queryId);
      this.session = Preconditions.checkNotNull(session);
      this.dContext = Preconditions.checkNotNull(dContext);
      this.req = Preconditions.checkNotNull(req);

    }

    @Override
    public double plan() throws Exception {
      return 1;
    }

    @Override
    public CommandType getCommandType() {
      return CommandType.SYNC_RESPONSE;
    }

    @Override
    public String getDescription() {
      return "server metadata; direct";
    }

    @Override
    public GetServerMetaResp execute() throws Exception {
      final GetServerMetaResp.Builder respBuilder = GetServerMetaResp.newBuilder();
      final ServerMeta.Builder metaBuilder = session.getRecordBatchFormat() != RecordBatchFormat.DRILL_1_0
          ? ServerMeta.newBuilder(DEFAULT)
          : ServerMeta.newBuilder(DRILL_1_0_DEFAULT);
      PlannerSettings plannerSettings = new PlannerSettings(dContext.getConfig(), session.getOptions(), dContext.getClusterResourceInformation());

      ParserConfig config = ParserConfig.newInstance(session, plannerSettings);

      int identifierMaxLength = config.identifierMaxLength();
      Metadata metadata = SqlParser.create("", config).getMetadata();
      metaBuilder
        .setMaxCatalogNameLength(identifierMaxLength)
        .setMaxColumnNameLength(identifierMaxLength)
        .setMaxCursorNameLength(identifierMaxLength)
        .setMaxSchemaNameLength(identifierMaxLength)
        .setMaxTableNameLength(identifierMaxLength)
        .setMaxUserNameLength(identifierMaxLength)
        .setIdentifierQuoteString(config.quoting().string)
        .setIdentifierCasing(getIdentifierCasing(config.unquotedCasing(), config.caseSensitive()))
        .setQuotedIdentifierCasing(getIdentifierCasing(config.quotedCasing(), config.caseSensitive()))
        .addAllSqlKeywords(Splitter.on(",").split(metadata.getJdbcKeywords()));
      respBuilder.setServerMeta(metaBuilder);
      respBuilder.setStatus(RequestStatus.OK);
      respBuilder.setQueryId(queryId);
      return respBuilder.build();
    }

    public static IdentifierCasing getIdentifierCasing(Casing casing, boolean caseSensitive) {
      switch(casing) {
      case TO_LOWER:
        return IdentifierCasing.IC_STORES_LOWER;

      case TO_UPPER:
        return IdentifierCasing.IC_STORES_UPPER;

      case UNCHANGED:
        return caseSensitive ? IdentifierCasing.IC_SUPPORTS_MIXED : IdentifierCasing.IC_STORES_MIXED;

      default:
        throw new AssertionError("Unknown casing:" + casing);
      }
    }
  }
}
