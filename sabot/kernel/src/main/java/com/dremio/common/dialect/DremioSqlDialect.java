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
package com.dremio.common.dialect;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.dialect.arp.transformer.NoOpTransformer;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.rel2sql.DremioRelToSqlConverter;
import com.dremio.exec.expr.fn.FunctionRegistry;
import com.dremio.exec.planner.sql.SqlOperatorImpl;

/**
 * DremioSqlDialect is an extension of Calcite's SqlDialect with
 * additional translation features and an optional dedicated DremioRelToSqlConverter.
 */
public class DremioSqlDialect extends org.apache.calcite.sql.SqlDialect {

  public static final DremioSqlDialect CALCITE = new DremioSqlDialect(
    DatabaseProduct.CALCITE.name(),
    "\"",
    NullCollation.HIGH);

  public static final DremioSqlDialect HIVE = new DremioSqlDialect(
    DatabaseProduct.HIVE.name(),
    "`",
    NullCollation.HIGH);

  // Custom functions
  private static final SqlFunction PI_FUNCTION =
    new SqlFunction("PI", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE,
      InferTypes.FIRST_KNOWN, OperandTypes.NILADIC, SqlFunctionCategory.NUMERIC);

  protected static final SqlFunction DATEDIFF = new SqlFunction(
    "DATEDIFF", SqlStdOperatorTable.TIMESTAMP_DIFF.getKind(),
    SqlStdOperatorTable.TIMESTAMP_DIFF.getReturnTypeInference(),
    SqlStdOperatorTable.TIMESTAMP_DIFF.getOperandTypeInference(),
    SqlStdOperatorTable.TIMESTAMP_DIFF.getOperandTypeChecker(),
    SqlStdOperatorTable.TIMESTAMP_DIFF.getFunctionType());

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSqlDialect.class);

  protected final String databaseName;

  private static DatabaseProduct getDbProduct(String databaseProductName) {
    try {
      return DatabaseProduct.valueOf(databaseProductName.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      return DatabaseProduct.UNKNOWN;
    }
  }

  public enum ContainerSupport {
    AUTO_DETECT,
    SUPPORTED,
    UNSUPPORTED
  }

  protected DremioSqlDialect(String databaseProductName,
                             String identifierQuoteString,
                             NullCollation nullCollation) {
    super(getDbProduct(databaseProductName), databaseProductName, identifierQuoteString, nullCollation);
    this.databaseName = databaseProductName.toUpperCase(Locale.ROOT);
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  // Calcite overrides
  @Override
  public boolean supportsCharSet() {
    return false;
  }

  @Override
  public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
    // Non-ARP dialects do not allow UDFs but do allow everything else.
    // TODO: DX-13199. Some functions such as Flatten are Dremio functions but not subclasses
    // of SqlOperatorImpl so they could pass this check and we'll try to push them down.
    return !(operator instanceof SqlOperatorImpl);
  }

  @Override
  public boolean supportsOrderByNullOrdering() {
    return false;
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    // Translate the PI function call to PI()
    if (call.getOperator() == SqlStdOperatorTable.PI) {
      super.unparseCall(
        writer,
        PI_FUNCTION.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)),
        leftPrec, rightPrec);
    } else if (call.getOperator().equals(FunctionRegistry.E_FUNCTION)) {
      // Translate the E() function call to EXP(1)
      final SqlCall newCall = SqlStdOperatorTable.EXP.createCall(
        SqlParserPos.ZERO, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
      super.unparseCall(writer, newCall, leftPrec, rightPrec);
    } else if (call.getKind() == SqlKind.JOIN) {
      this.unparseJoin(writer, (SqlJoin) call, leftPrec, rightPrec);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  // Dremio-specific
  public DremioRelToSqlConverter getConverter() {
    return new DremioRelToSqlConverter(this);
  }

  public boolean supportsLiteral(CompleteType type) {
    return true;
  }

  public boolean supportsBooleanAggregation() {
    return true;
  }

  public boolean supportsAggregation() {
    return true;
  }

  public boolean supportsDistinct() {
    return true;
  }

  public boolean supportsCount(AggregateCall call) {
    return true;
  }

  public boolean supportsFunction(AggregateCall aggCall, List<RelDataType> paramTypes) {
    return true;
  }

  public boolean supportsSubquery() {
    return true;
  }

  public boolean supportsCorrelatedSubquery() {
    return true;
  }

  /**
   * Indicates if the given operator, which takes in a time unit literal, is supported with
   * the given time unit.
   * <p>
   * The operands parameter includes the TimeUnit itself.
   */
  public boolean supportsTimeUnitFunction(SqlOperator operator, TimeUnitRange timeUnit, RelDataType returnType,
                                          List<RelDataType> paramTypes) {
    return supportsFunction(operator, returnType, paramTypes);
  }

  public boolean supportsUnion() {
    return true;
  }

  public boolean supportsUnionAll() {
    return true;
  }

  /**
   * Get the name of the dummy table used to implement a select on VALUES query.
   * If null, just use a VALUES statement.
   */
  public String getValuesDummyTable() {
    return null;
  }

  /**
   * Indicate if an explicit CAST should be added for all decimal columns.
   *
   * @return
   */
  public boolean shouldInjectNumericCastToProject() {
    return true;
  }

  /**
   * Indicate if an explicit CAST should be added for all approximate numeric columns.
   *
   * @return
   */
  public boolean shouldInjectApproxNumericCastToProject() {
    return false;
  }

  /**
   * Gets the SqlCollation node that should applied to string columns that is specific to this
   * data source.
   *
   * @param kind The type of SqlNode the collation would get applied to.
   */
  public SqlCollation getDefaultCollation(SqlKind kind) {
    return null;
  }

  /**
   * Indicates if the dialect supports sort.
   *
   * @param sort The sort description.
   */
  public boolean supportsSort(Sort sort) {
    return this.supportsSort(isCollationEmpty(sort), isOffsetEmpty(sort));
  }

  public static boolean isCollationEmpty(Sort sort) {
    return sort.getCollation() == null || sort.getCollation() == RelCollations.EMPTY;
  }

  public static boolean isOffsetEmpty(Sort sort) {
    return sort.offset == null || ((long) ((RexLiteral) sort.offset).getValue2()) == 0L;
  }

  /**
   * Indicates if the dialect supports sort.
   *
   * @param isCollationEmpty Indicates if the collation used by the Sort is empty.
   * @param isOffsetEmpty    Indicates if the OFFSET is empty.
   * @return True if sort is supported with the given conditions. False otherwise.
   */
  public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
    return false;
  }

  public boolean supportsFetchOffsetInSetOperand() {
    return true;
  }

  /**
   * Indicates if the dialect supports the given join type.
   */
  public boolean supportsJoin(JoinType type) {
    return true;
  }

  /**
   * Generates dialect-specific syntax for a join.
   */
  public void unparseJoin(SqlWriter writer, SqlJoin join, int leftPrec, int rightPrec) {
    super.unparseCall(writer, join, leftPrec, rightPrec);
  }

  /**
   * Indicates if character data is automatically trimmed of trailing space in the data source.
   */
  public boolean requiresTrimOnChars() {
    return false;
  }

  /**
   * Indicates that the RDBMS can implement the given OVER clause
   *
   * @param over
   * @return True if the RDBMS can implement the given OVER clause.
   */
  public boolean supportsOver(RexOver over) {
    return true;
  }

  /**
   * Indicates that the RDBMS can implement the given Window Rel
   *
   * @param window
   * @return True if the RDBMS can implement the given Window Rel.
   */
  public boolean supportsOver(Window window) {
    return true;
  }

  /**
   * Time values within results have any provided timezone information removed
   * while keeping the value the same. For instance 1pm PST is turned into 1pm
   * UTC. This is useful for any driver that is sending the correct UTC time
   * value, however the time zone is the default for the system.
   */
  public boolean coerceTimesToUTC() {
    return false;
  }

  /**
   * Timestamp values within results have any provided timezone information
   * removed while keeping the value the same. For instance 1pm PST is turned into
   * 1pm UTC. This is useful for any driver that is sending the correct UTC time
   * value, however the time zone is the default for the system.
   */
  public boolean coerceTimestampsToUTC() {
    return false;
  }

  /**
   * Date values within results may have a timezone offset without properly adjusting
   * the hourly values, resulting in dates that are incorrect for comparisons and
   * differences although appearing correct through the UI. This setting accounts for
   * this and readjusts the value to be correct, effectively cancelling out the timezone.
   */
  public boolean adjustDateTimezone() {
    return false;
  }

  /**
   * Determine if the supplied Dremio-specific datetime format string can be supported by the RDBMS by mapping it to
   * the RDBMS specific format string.
   *
   * @param dateTimeFormatStr The Dremio datetime format string to check support for.
   * @return True if the datetime format string can be fully mapped to the RDBMS format string.
   */
  public boolean supportsDateTimeFormatString(String dateTimeFormatStr) {
    return true;
  }

  /**
   * Determine if the supplied Dremio-specific regex string can be supported by the RDBMS by mapping it to
   * the RDBMS specific regex string.
   *
   * @param regex The Dremio regex string to check support for.
   * @return True if the regex string can be fully mapped to the RDBMS regex string.
   */
  public boolean supportsRegexString(String regex) {
    return true;
  }

  /**
   * Indicates if the window frames Calcite automatically generates should be removed
   * in OVER clauses.
   */
  public boolean removeDefaultWindowFrame(RexOver over) {
    return true;
  }

  /**
   * Dialect callback when a new SqlNode has been created based on a RexNode.
   */
  public SqlNode decorateSqlNode(RexNode rexNode, Supplier<SqlNode> defaultNodeSupplier) {
    return defaultNodeSupplier.get();
  }

  /**
   * Dialect callback when a new SqlNode has been created based on an AggregateCall and its parameters.
   */
  public SqlNode decorateSqlNode(AggregateCall aggCall, Supplier<List<RelDataType>> argTypes, Supplier<SqlNode> defaultNodeSupplier) {
    return defaultNodeSupplier.get();
  }

  /**
   * Gets a transformer to adjust the given RexCall
   */
  public CallTransformer getCallTransformer(RexCall call) {
    return NoOpTransformer.INSTANCE;
  }

  /**
   * Gets a transformer to adjust the given RexCall
   */
  public CallTransformer getCallTransformer(SqlOperator op) {
    return NoOpTransformer.INSTANCE;
  }

  /**
   * Determines if the input node has a boolean literal or a call that returns booleans.
   *
   * @param node                    The node to explore.
   * @param rexCallCanReturnBoolean Indicates that the node is permitted to have a child which is a call that returns a boolean.
   * @return
   */
  public boolean hasBooleanLiteralOrRexCallReturnsBoolean(RexNode node, boolean rexCallCanReturnBoolean) {
    final SqlTypeName nodeDataType = node.getType().getSqlTypeName();
    if (node instanceof RexLiteral) {
      final boolean toReturn = nodeDataType == SqlTypeName.BOOLEAN;
      if (toReturn) {
        logger.debug("Boolean RexLiteral found, {}", node);
      }
      return toReturn;
    }
    if (node instanceof RexInputRef) {
      return false;
    }
    if (node instanceof RexCall) {
      final RexCall call = (RexCall) node;
      if (nodeDataType == SqlTypeName.BOOLEAN
        && (!rexCallCanReturnBoolean || call.getOperator().getKind() == SqlKind.CAST)) {
        logger.debug("RexCall returns boolean, {}", node);
        return true;
      }
      final List<RexNode> operandsToCheck;
      if ((call.getOperator().getKind() == SqlKind.CASE)) {
        assert call.getOperands().size() >= 2;
        operandsToCheck = call.getOperands().subList(1, call.getOperands().size());
      } else {
        operandsToCheck = call.getOperands();
      }

      for (RexNode operand : operandsToCheck) {
        if (hasBooleanLiteralOrRexCallReturnsBoolean(operand, rexCallCanReturnBoolean)) {
          logger.debug("RexCall has boolean inputs, {}", node);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the limit on the length of an identifier for this dialect.
   */
  public Integer getIdentifierLengthLimit() {
    return null;
  }

  public ContainerSupport supportsCatalogs() {
    return ContainerSupport.AUTO_DETECT;
  }

  public ContainerSupport supportsSchemas() {
    return ContainerSupport.AUTO_DETECT;
  }

  protected static SqlNode getVarcharWithPrecision(DremioSqlDialect dialect, RelDataType type, int precision) {
    return new SqlDataTypeSpec(
      new SqlIdentifier(type.getSqlTypeName().name(), SqlParserPos.ZERO),
      precision,
      type.getScale(),
      type.getCharset() != null && dialect.supportsCharSet()
        ? type.getCharset().name() : null,
      null,
      SqlParserPos.ZERO);
  }
}
