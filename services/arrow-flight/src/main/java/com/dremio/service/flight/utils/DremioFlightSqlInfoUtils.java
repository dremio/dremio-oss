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
package com.dremio.service.flight.utils;

import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions.SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions.SQL_ELEMENT_IN_PROCEDURE_CALLS;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions.SQL_UNION_ALL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel.ANSI92_FULL_SQL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar.SQL_CORE_GRAMMAR;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar.SQL_EXTENDED_GRAMMAR;
import static org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar.SQL_MINIMUM_GRAMMAR;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;

import com.dremio.common.types.TypeProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserProtos;
import com.dremio.sabot.rpc.user.UserRpcUtils;

/**
 * A collection of helper methods to build a {@link FlightSql.SqlInfo}.
 */
public final class DremioFlightSqlInfoUtils {

  private DremioFlightSqlInfoUtils() {
  }

  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  /**
   * Builds a new {@link SqlInfoBuilder} object based on {@link UserProtos.ServerMeta}.
   *
   * @param serverMeta the {@link UserProtos.ServerMeta} to be used to provide server metadata.
   * @return a filled {@link SqlInfoBuilder}
   */
  public static SqlInfoBuilder getNewSqlInfoBuilder(final UserProtos.ServerMeta serverMeta) {
    final UserBitShared.RpcEndpointInfos serverInfo = UserRpcUtils.getRpcEndpointInfos("Dremio Server");

    return new SqlInfoBuilder()
      .withSqlOuterJoinSupportLevel(FlightSql.SqlOuterJoinsSupportLevel.SQL_FULL_OUTER_JOINS)
      .withFlightSqlServerName(serverInfo.getName())
      .withFlightSqlServerVersion(UserRpcUtils.getVersion(serverInfo).getVersion())
      .withSqlIdentifierQuoteChar(serverMeta.getIdentifierQuoteString())
      .withFlightSqlServerReadOnly(serverMeta.getReadOnly())
      .withSqlKeywords(serverMeta.getSqlKeywordsList().toArray(EMPTY_STRING_ARRAY))
      .withSqlNumericFunctions(serverMeta.getNumericFunctionsList().toArray(EMPTY_STRING_ARRAY))
      .withSqlStringFunctions(serverMeta.getStringFunctionsList().toArray(EMPTY_STRING_ARRAY))
      .withSqlSystemFunctions(serverMeta.getSystemFunctionsList().toArray(EMPTY_STRING_ARRAY))
      .withSqlDatetimeFunctions(serverMeta.getDateTimeFunctionsList().toArray(EMPTY_STRING_ARRAY))
      .withSqlSearchStringEscape(serverMeta.getSearchEscapeString())
      .withSqlExtraNameCharacters(serverMeta.getSpecialCharacters())
      .withSqlSupportsColumnAliasing(serverMeta.getColumnAliasingSupported())
      .withSqlNullPlusNullIsNull(serverMeta.getNullPlusNonNullEqualsNull())
      .withSqlSupportsTableCorrelationNames(
        serverMeta.getCorrelationNamesSupport() == UserProtos.CorrelationNamesSupport.CN_ANY
          || serverMeta.getCorrelationNamesSupport() == UserProtos.CorrelationNamesSupport.CN_DIFFERENT_NAMES)
      .withSqlSupportsDifferentTableCorrelationNames(
        serverMeta.getCorrelationNamesSupport() == UserProtos.CorrelationNamesSupport.CN_DIFFERENT_NAMES)
      .withSqlSupportsExpressionsInOrderBy(
        serverMeta.getOrderBySupportList().contains(UserProtos.OrderBySupport.OB_EXPRESSION))
      .withSqlSupportsOrderByUnrelated(
        serverMeta.getOrderBySupportList().contains(UserProtos.OrderBySupport.OB_UNRELATED))
      .withSqlSupportedGroupBy(getGroupBy(serverMeta.getGroupBySupport()))
      .withSqlSupportsLikeEscapeClause(serverMeta.getLikeEscapeClauseSupported())
      .withSqlSchemaTerm(serverMeta.getSchemaTerm())
      .withSqlCatalogTerm("")  // Indicates catalogs are not supported
      .withSqlCatalogAtStart(serverMeta.getCatalogAtStart())
      .withSqlSupportedPositionedCommands(SQL_POSITIONED_DELETE)
      .withSqlSelectForUpdateSupported(serverMeta.getSelectForUpdateSupported())
      .withSqlSubQueriesSupported(getSubQuerySupport(serverMeta.getSubquerySupportList()))
      .withSqlCorrelatedSubqueriesSupported(
        serverMeta.getSubquerySupportList().contains(UserProtos.SubQuerySupport.SQ_CORRELATED))
      .withSqlSupportedUnions(getSupportedUnions(serverMeta.getUnionSupportList()))
      .withSqlMaxBinaryLiteralLength(serverMeta.getMaxBinaryLiteralLength())
      .withSqlMaxCharLiteralLength(serverMeta.getMaxCharLiteralLength())
      .withSqlMaxColumnNameLength(serverMeta.getMaxColumnNameLength())
      .withSqlMaxColumnsInGroupBy(serverMeta.getMaxColumnsInGroupBy())
      .withSqlMaxColumnsInOrderBy(serverMeta.getMaxColumnsInOrderBy())
      .withSqlMaxColumnsInSelect(serverMeta.getMaxColumnsInSelect())
      .withSqlMaxCursorNameLength(serverMeta.getMaxCursorNameLength())
      .withSqlDbSchemaNameLength(serverMeta.getMaxSchemaNameLength())
      .withSqlMaxCatalogNameLength(serverMeta.getMaxCatalogNameLength())
      .withSqlMaxRowSize(serverMeta.getMaxRowSize())
      .withSqlMaxRowSizeIncludesBlobs(serverMeta.getBlobIncludedInMaxRowSize())
      .withSqlMaxStatementLength(serverMeta.getMaxStatementLength())
      .withSqlMaxStatements(serverMeta.getMaxStatements())
      .withSqlMaxTableNameLength(serverMeta.getMaxTableNameLength())
      .withSqlMaxTablesInSelect(serverMeta.getMaxTablesInSelect())
      .withSqlMaxUsernameLength(serverMeta.getMaxUserNameLength())
      .withSqlSupportsConvert(getSupportsConvert(serverMeta.getConvertSupportList()))
      .withSqlSupportedResultSetTypes(FlightSql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY)

      .withSqlSupportedGrammar(SQL_CORE_GRAMMAR, SQL_MINIMUM_GRAMMAR, SQL_EXTENDED_GRAMMAR) // Same as AvaticaDatabaseMetaData#supports**SQLGrammar
      .withSqlAnsi92SupportedLevel(ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL) // Same as AvaticaDatabaseMetaData#supportsANSI92**
      .withSqlSchemasSupportedActions(SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS,
        SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS) // Same as AvaticaDatabaseMetaData#supportsSchemasIn*
      .withSqlCatalogsSupportedActions(SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS,
        SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS) // Same as AvaticaDatabaseMetaData#supportsCatalogsIn*
      .withSqlMaxColumnsInIndex(0) // Same as AvaticaDatabaseMetaData#getMaxColumnsInIndex
      .withSqlMaxConnections(0) // Same as AvaticaDatabaseMetaData#getMaxConnections
      .withSqlMaxIndexLength(0) // Same as AvaticaDatabaseMetaData#getMaxIndexLength
      .withSqlMaxProcedureNameLength(0) // Same as AvaticaDatabaseMetaData#getMaxProcedureNameLength
      .withSqlProcedureTerm("procedure") // Same as AvaticaDatabaseMetaData#getProcedureTerm
      .withSqlSupportsNonNullableColumns(true) // Same as AvaticaDatabaseMetaData#supportsNonNullableColumns
      .withSqlStoredProceduresSupported(false) // Same as AvaticaDatabaseMetaData#supportsStoredProcedures*
      .withSqlSupportsIntegrityEnhancementFacility(false) // Same as AvaticaDatabaseMetaData#supportsIntegrityEnhancementFacility
      .withSqlBatchUpdatesSupported(true) // Same as AvaticaDatabaseMetaData#supportsBatchUpdates
      .withSqlSavepointsSupported(false) // Same as AvaticaDatabaseMetaData#supportsSavepoints
      .withSqlNamedParametersSupported(false) // Same as AvaticaDatabaseMetaData#supportsNamedParameters
      .withSqlLocatorsUpdateCopy(true) // Same as AvaticaDatabaseMetaData#locatorsUpdateCopy
      .withSqlStoredFunctionsUsingCallSyntaxSupported(true) // Same as AvaticaDatabaseMetaData#supportsStoredFunctionsUsingCallSyntax

      .withSqlDefaultTransactionIsolation(0) // Flight SQL doesn't support Transactions
      .withSqlTransactionsSupported(false) // Flight SQL doesn't support Transactions
      .withSqlSupportedTransactionsIsolationLevels(FlightSql.SqlTransactionIsolationLevel.SQL_TRANSACTION_NONE) // Flight SQL doesn't support Transactions
      .withSqlDataDefinitionCausesTransactionCommit(false) // Flight SQL doesn't support Transactions
      .withSqlDataDefinitionsInTransactionsIgnored(false); // Flight SQL doesn't support Transactions
  }

  public static Map<Integer, List<Integer>> getSupportsConvert(final List<UserProtos.ConvertSupport> convertSupports) {
    final Map<Integer, List<Integer>> conversionMap = new HashMap<>();
    for (final UserProtos.ConvertSupport convertSupport : convertSupports) {
      int from = getSqlTypeFromMinorType(convertSupport.getFrom());
      int to = getSqlTypeFromMinorType(convertSupport.getTo());
      if (!conversionMap.containsKey(from)) {
        conversionMap.put(from, new ArrayList<>());
      }
      conversionMap.get(from).add(to);
    }
    return conversionMap;
  }

  public static int getSqlTypeFromMinorType(final TypeProtos.MinorType minorType) throws UnsupportedOperationException {
    switch (minorType) {
      case BIT:
        return Types.BIT;
      case UINT1:
      case TINYINT:
        return Types.TINYINT;
      case UINT2:
      case SMALLINT:
        return Types.SMALLINT;
      case UINT4:
      case INT:
        return Types.INTEGER;
      case UINT8:
      case BIGINT:
        return Types.BIGINT;
      case DATE:
        return Types.DATE;
      case TIME:
        return Types.TIME;
      case TIMETZ:
        return Types.TIME_WITH_TIMEZONE;
      case FLOAT4:
        return Types.FLOAT;
      case FLOAT8:
        return Types.DOUBLE;
      case MONEY:
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL:
        return Types.DECIMAL;
      case FIXEDCHAR:
      case FIXED16CHAR:
        return Types.CHAR;
      case VARCHAR:
      case VAR16CHAR:
        return Types.VARCHAR;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case TIMESTAMPTZ:
        return Types.TIMESTAMP_WITH_TIMEZONE;
      case FIXEDSIZEBINARY:
        return Types.BINARY;
      case VARBINARY:
        return Types.VARBINARY;
      case LIST:
        return Types.ARRAY;
      case STRUCT:
        return Types.STRUCT;
      case NULL:
        return Types.NULL;
      case MAP:
        return Types.OTHER;
      case LATE:
      case UNION:
      case INTERVAL:
      case INTERVALDAY:
      case INTERVALYEAR:
      case GENERIC_OBJECT:
        return Types.JAVA_OBJECT;
      default:
        throw new UnsupportedOperationException("'" + minorType + "' is not a supported MinorType in Flight SQL.");
    }
  }

  public static FlightSql.SqlSupportedUnions[] getSupportedUnions(final List<UserProtos.UnionSupport> supports) {
    final List<FlightSql.SqlSupportedUnions> returnList = new ArrayList<>();
    for (final UserProtos.UnionSupport unionSupport : supports) {
      switch (unionSupport) {
        case U_UNION_ALL:
          returnList.add(SQL_UNION_ALL);
          break;
        case U_UNION:
          returnList.add(FlightSql.SqlSupportedUnions.SQL_UNION);
          break;
        case U_UNKNOWN:
          returnList.add(FlightSql.SqlSupportedUnions.UNRECOGNIZED);
          break;
        default:
          break;
      }
    }
    return returnList.toArray(new FlightSql.SqlSupportedUnions[0]);
  }

  public static FlightSql.SqlSupportedGroupBy getGroupBy(final UserProtos.GroupBySupport groupBySupport) {
    switch (groupBySupport) {
      case GB_NONE:
      case GB_SELECT_ONLY:
        return FlightSql.SqlSupportedGroupBy.UNRECOGNIZED;
      case GB_UNRELATED:
        return FlightSql.SqlSupportedGroupBy.SQL_GROUP_BY_UNRELATED;
      case GB_BEYOND_SELECT:
        return FlightSql.SqlSupportedGroupBy.SQL_GROUP_BY_BEYOND_SELECT;
      default:
        throw new UnsupportedOperationException("'" + groupBySupport + " is not a supported FlightSql.SqlSupportedGroupBy type.");
    }
  }

  public static FlightSql.SqlSupportedSubqueries[] getSubQuerySupport(
    final List<UserProtos.SubQuerySupport> userProtoSubQuerySupport) {
    final List<FlightSql.SqlSupportedSubqueries> flightSqlSubQuerySupport = new ArrayList<>();
    final FlightSql.SqlSupportedSubqueries[] emptyFlightSqlSubQuerySupportArr = new FlightSql.SqlSupportedSubqueries[0];

    if (userProtoSubQuerySupport == null || userProtoSubQuerySupport.isEmpty()) {
      flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.UNRECOGNIZED);
      return flightSqlSubQuerySupport.toArray(emptyFlightSqlSubQuerySupportArr);
    }

    for (final UserProtos.SubQuerySupport subQuerySupport : userProtoSubQuerySupport) {
      switch (subQuerySupport) {
        case SQ_IN_EXISTS:
          flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.SQL_SUBQUERIES_IN_EXISTS);
          break;
        case SQ_IN_INSERT:
          flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.SQL_SUBQUERIES_IN_INS);
          break;
        case SQ_IN_COMPARISON:
          flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.SQL_SUBQUERIES_IN_COMPARISONS);
          break;
        case SQ_IN_QUANTIFIED:
          flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.SQL_SUBQUERIES_IN_QUANTIFIEDS);
          break;
        case SQ_UNKNOWN:
          flightSqlSubQuerySupport.add(FlightSql.SqlSupportedSubqueries.UNRECOGNIZED);
          break;
        case SQ_CORRELATED:
          // pass; Correlated sub queries are not part of this enum
          break;
        default:
          throw new UnsupportedOperationException("'" + subQuerySupport + "' is not a supported FlightSql.SqlSupportedSubqueries type.");
      }
    }

    return flightSqlSubQuerySupport.toArray(emptyFlightSqlSubQuerySupportArr);
  }
}
