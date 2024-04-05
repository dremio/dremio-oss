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
package com.dremio.exec.planner.serializer;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.sql.SqlFlattenOperator;
import com.dremio.plan.serialization.PBoundOption;
import com.dremio.plan.serialization.PRelList;
import com.dremio.plan.serialization.PRexCall;
import com.dremio.plan.serialization.PRexCorrelVariable;
import com.dremio.plan.serialization.PRexDynamicParam;
import com.dremio.plan.serialization.PRexFieldAccess;
import com.dremio.plan.serialization.PRexFieldCollation;
import com.dremio.plan.serialization.PRexInputRef;
import com.dremio.plan.serialization.PRexLiteral;
import com.dremio.plan.serialization.PRexLocalRef;
import com.dremio.plan.serialization.PRexNode;
import com.dremio.plan.serialization.PRexOver;
import com.dremio.plan.serialization.PRexPatternFieldRef;
import com.dremio.plan.serialization.PRexRangeRef;
import com.dremio.plan.serialization.PRexSubQuery;
import com.dremio.plan.serialization.PRexWindow;
import com.dremio.plan.serialization.PRexWindowBound;
import com.dremio.plan.serialization.PSqlTypeName;
import com.dremio.plan.serialization.PSymbol;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

class RexDeserializer {
  private static final SqlCollation Utf8SqlCollation =
      new SqlCollation("UTF-8$en_US$primary", SqlCollation.Coercibility.IMPLICIT);

  private final RexBuilder rexBuilder;
  private final TypeSerde types;
  private final RelSerdeRegistry registry;
  private final RelNodeSerde.TableRetriever tables;
  private final RelNodeSerde.PluginRetriever plugins;
  private final RelOptCluster cluster;

  private final SqlOperatorSerde sqlOperatorSerde;

  public RexDeserializer(
      RexBuilder rexBuilder,
      TypeSerde types,
      RelSerdeRegistry registry,
      RelNodeSerde.TableRetriever tables,
      RelNodeSerde.PluginRetriever plugins,
      RelOptCluster cluster,
      SqlOperatorSerde sqlOperatorSerde) {
    super();
    this.rexBuilder = rexBuilder;
    this.types = types;
    this.registry = registry;
    this.tables = tables;
    this.plugins = plugins;
    this.cluster = cluster;
    this.sqlOperatorSerde = sqlOperatorSerde;
  }

  public RexNode convert(PRexNode node, RelDataType rowType) {
    switch (node.getRexTypeCase()) {
      case REX_CALL:
        return convertCall(node.getRexCall());

      case REX_INPUT_REF:
        return convertInputRef(node.getRexInputRef(), rowType);

      case REX_LITERAL:
        return convertLiteral(node.getRexLiteral());

      case REX_LOCAL_REF:
        return convertLocalRef(node.getRexLocalRef());

      case REX_OVER:
        return convertOver(node.getRexOver());

      case REX_FIELD_ACCESS:
        return convertFieldAccess(node.getRexFieldAccess());

      case REX_CORREL_VARIABLE:
        return convertCorrelVariable(node.getRexCorrelVariable());

      case REX_SUBQUERY:
        return convertSubQuery(node.getRexSubquery());

      case REX_DYNAMIC_PARAM:
        return convertDynamicParam(node.getRexDynamicParam());

      case REX_RANGE_REF:
        return convertRangeRef(node.getRexRangeRef());

      case REX_PATTERN_FIELD_REF:
        return convertPatternFieldRef(node.getRexPatternFieldRef());

      case REXTYPE_NOT_SET:
      default:
        break;
    }
    throw new IllegalStateException("Unsupported type case: " + node.getRexTypeCase());
  }

  public RexNode convert(PRexNode node) {
    return convert(node, null);
  }

  private RexNode convertSubQuery(PRexSubQuery subQuery) {
    SqlOperator operator = sqlOperatorSerde.fromProto(subQuery.getSqlOperator());
    PRelList list = PRelList.newBuilder().addAllNode(subQuery.getDetailsList()).build();
    final RelNode rel =
        RelDeserializer.deserialize(
            registry,
            DremioRelFactories.CALCITE_LOGICAL_BUILDER,
            tables,
            plugins,
            list,
            cluster,
            sqlOperatorSerde);
    CorrelationId correlationId = convertCorrelationId(subQuery.getCorrelationId());
    switch (operator.getKind()) {
      case IN:
      case NOT_IN:
        return RexSubQuery.in(
            rel,
            ImmutableList.copyOf(
                subQuery.getOperandsList().stream()
                    .map(this::convert)
                    .collect(Collectors.toList())),
            correlationId);
      case EXISTS:
        return RexSubQuery.exists(rel, correlationId);
      case SCALAR_QUERY:
        return RexSubQuery.scalar(rel, correlationId);
      case SOME:
        return RexSubQuery.some(
            rel,
            ImmutableList.copyOf(
                subQuery.getOperandsList().stream()
                    .map(this::convert)
                    .collect(Collectors.toList())),
            (SqlQuantifyOperator) operator,
            correlationId);
      case ARRAY_QUERY_CONSTRUCTOR:
        return RexSubQuery.array(rel, correlationId);
      default:
        throw new IllegalStateException("Unsupported subquery type: " + operator.getKind());
    }
  }

  private CorrelationId convertCorrelationId(@Nullable Integer correlationId) {
    if (correlationId == null) {
      return null;
    }
    return new CorrelationId(correlationId);
  }

  private RexNode convertCorrelVariable(PRexCorrelVariable fieldAccess) {
    return rexBuilder.makeCorrel(
        types.fromProto(fieldAccess.getDataType()),
        new CorrelationId(fieldAccess.getCorrelationId()));
  }

  private RexNode convertFieldAccess(PRexFieldAccess fieldAccess) {
    return rexBuilder.makeFieldAccess(
        convert(fieldAccess.getExpression()), fieldAccess.getField().getIndex());
  }

  private RexNode convertOver(PRexOver over) {
    PRexWindow window = over.getRexWindow();
    boolean isRows = window.getIsRows();

    List<RexNode> partitionKeys =
        window.getPartitionKeysList().stream()
            .map(p -> this.convert(p))
            .collect(Collectors.toList());
    List<RexFieldCollation> orderKeys =
        window.getOrderKeysList().stream().map(this::convert).collect(Collectors.toList());

    return rexBuilder.makeOver(
        types.fromProto(over.getDataType()),
        (SqlAggFunction) sqlOperatorSerde.fromProto(over.getSqlOperator()),
        over.getOperandsList().stream().map(o -> this.convert(o)).collect(Collectors.toList()),
        partitionKeys,
        ImmutableList.copyOf(orderKeys),
        window.hasLowerBound() ? convert(window.getLowerBound()) : null,
        window.hasUpperBound() ? convert(window.getUpperBound()) : null,
        isRows, // physical,
        true, // allowPartial,
        false, // nullWhenCountZero,
        over.getDistinct());
  }

  public RexFieldCollation convert(PRexFieldCollation pRexFieldCollation) {
    RexNode left = this.convert(pRexFieldCollation.getLeft());
    Set<SqlKind> right = new HashSet<>();
    for (PRexFieldCollation.PSortFlag flag : pRexFieldCollation.getRightList()) {
      SqlKind kind = this.convert(flag);
      right.add(kind);
    }

    return new RexFieldCollation(left, right);
  }

  private SqlKind convert(PRexFieldCollation.PSortFlag pSortFlag) {
    switch (pSortFlag) {
      case DESCENDING:
        return SqlKind.DESCENDING;
      case NULLS_FIRST:
        return SqlKind.NULLS_FIRST;
      case NULLS_LAST:
        return SqlKind.NULLS_LAST;
      default:
        throw new UnsupportedOperationException("Unknown type: " + pSortFlag);
    }
  }

  private RexWindowBound convert(PRexWindowBound bound) {
    SqlNode sqlNode;
    RexNode rexNode;
    switch (bound.getRexWindowBoundCase()) {
      case BOUNDED:
        RexNode offset = convert(bound.getBounded().getOffset());
        sqlNode = SqlNodeList.EMPTY;
        rexNode = offset;
        break;

      case CURRENT_ROW:
        sqlNode = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
        rexNode = null;
        break;

      case UNBOUNDED:
        sqlNode =
            bound.getUnbounded().getBoundOption() == PBoundOption.FOLLOWING
                ? SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO)
                : SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
        rexNode = null;
        break;

      case REXWINDOWBOUND_NOT_SET:
      default:
        throw new UnsupportedOperationException("Unknown type: " + bound.getRexWindowBoundCase());
    }

    return RexWindowBound.create(sqlNode, rexNode);
  }

  private RexNode convertInputRef(PRexInputRef inputRef, RelDataType rowType) {
    int index = inputRef.getIndex();
    final RelDataType dataType =
        rowType == null
            ? types.fromProto(inputRef.getDataType())
            : rowType.getFieldList().get(index).getType();
    return rexBuilder.makeInputRef(dataType, index);
  }

  private RexNode convertLocalRef(PRexLocalRef localRef) {
    return new RexLocalRef(localRef.getIndex(), types.fromProto(localRef.getDataType()));
  }

  private static void must(RelDataType actual, SqlTypeName... expected) {
    SqlTypeName a = actual.getSqlTypeName();
    for (SqlTypeName e : expected) {
      if (e == a) {
        return;
      }
    }

    throw new IllegalStateException(
        String.format(
            "Unexpected type. Expected one of types %s but type was actually %s",
            Arrays.toString(expected), actual));
  }

  private RexNode convertLiteral(PRexLiteral literal) {
    RelDataType type = types.fromProto(literal.getDataType());
    switch (literal.getValueTypeCase()) {
      case BINARY_VALUE:
        must(type, SqlTypeName.VARBINARY, SqlTypeName.BINARY);
        return rexBuilder.makeBinaryLiteral(
            new org.apache.calcite.avatica.util.ByteString(literal.getBinaryValue().toByteArray()));
      case BOOLEAN_VALUE:
        must(type, SqlTypeName.BOOLEAN);
        return rexBuilder.makeLiteral(literal.getBooleanValue());
      case DECIMAL_VALUE:
        BigInteger bi =
            new BigInteger(literal.getDecimalValue().getTwosComplementValue().toByteArray());
        BigDecimal bd = new BigDecimal(bi, literal.getDecimalValue().getScale());
        switch (type.getSqlTypeName()) {
          case BIGINT:
          case DECIMAL:
          case INTEGER:
          case SMALLINT:
          case TINYINT:
            return rexBuilder.makeExactLiteral(bd, type);
          case FLOAT:
          case DOUBLE:
            if (SqlTypeName.DECIMAL.equals(TypeSerde.fromProto(literal.getTypeName()))) {
              return rexBuilder.makeExactLiteral(bd, type);
            }
            return rexBuilder.makeApproxLiteral(bd, type);
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_MONTH:
          case INTERVAL_SECOND:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
            return rexBuilder.makeIntervalLiteral(bd, type.getIntervalQualifier());
          default:
            break;
        }
        break;
      case LONG_VALUE:
        switch (type.getSqlTypeName()) {
          case BIGINT:
          case INTEGER:
          case SMALLINT:
          case TINYINT:
          case FLOAT:
          case DOUBLE:
          case VARCHAR:
            return rexBuilder.makeLiteral(literal.getLongValue(), type, true);
          case DATE:
            return rexBuilder.makeDateLiteral(
                DateString.fromDaysSinceEpoch((int) (literal.getLongValue())));
          case TIMESTAMP:
            return rexBuilder.makeTimestampLiteral(
                TimestampString.fromMillisSinceEpoch(literal.getLongValue()), 3);
          case TIME:
            return rexBuilder.makeTimeLiteral(
                TimeString.fromMillisOfDay((int) literal.getLongValue()), type.getPrecision());
          default:
            break;
        }
        break;
      case STRING_VALUE:
        switch (type.getSqlTypeName()) {
          case CHAR:
          case DATE:
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_MONTH:
          case INTERVAL_SECOND:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case TIME:
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          case TIME_WITH_LOCAL_TIME_ZONE:
          case VARCHAR:
            try {
              return rexBuilder.makeLiteral(literal.getStringValue(), type, true);
            } catch (CalciteException ce) {
              if (!((type.getSqlTypeName() == SqlTypeName.CHAR)
                  || (type.getSqlTypeName() == SqlTypeName.VARCHAR))) {
                throw ce;
              }

              NlsString utf8NlsString =
                  new NlsString(literal.getStringValue(), "UTF-8", Utf8SqlCollation);
              RelDataType utf8RelDataType =
                  this.types
                      .getFactory()
                      .createTypeWithCharsetAndCollation(
                          type, StandardCharsets.UTF_8, Utf8SqlCollation);

              return rexBuilder.makeLiteral(utf8NlsString, utf8RelDataType, true);
            }

          default:
            break;
        }
        break;
      case SYMBOL_VALUE:
        return rexBuilder.makeFlag(makeEnum(literal.getSymbolValue()));
      case VALUETYPE_NOT_SET:
        if (type.isNullable()) {
          if (type.getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
            // match precision and scale for decimal null value
            return rexBuilder.makeExactLiteral(null, type);
          }
          return rexBuilder.makeNullLiteral(type);
        }
        break;
      default:
        break;
    }

    if (literal.getDataType().getTypeName() == PSqlTypeName.ANY) {
      return rexBuilder.makeNullLiteral(SqlTypeName.ANY);
    }
    throw new IllegalStateException("Unknown value handling: " + literal.getValueTypeCase());
  }

  private Enum makeEnum(PSymbol symbol) {
    String clazz = symbol.getClazz();
    try {
      return Enum.valueOf((Class<Enum>) Class.forName(clazz), symbol.getName());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  private RexNode convertCall(PRexCall call) {
    SqlOperator operator = sqlOperatorSerde.fromProto(call.getSqlOperator());
    if (operator instanceof SqlFlattenOperator) {
      operator = ((SqlFlattenOperator) operator).withIndex(call.getIndex());
    }

    RelDataType type = types.fromProto(call.getDataType());
    return rexBuilder.makeCall(
        type,
        operator,
        call.getOperandsList().stream().map(this::convert).collect(Collectors.toList()));
  }

  private RexNode convertDynamicParam(PRexDynamicParam dynamicParam) {
    return new RexDynamicParam(
        this.types.fromProto(dynamicParam.getRexVariable().getType()), dynamicParam.getIndex());
  }

  private RexNode convertRangeRef(PRexRangeRef rangeRef) {
    return rexBuilder.makeRangeReference(
        this.types.fromProto(rangeRef.getType()), rangeRef.getOffset(), false);
  }

  private RexNode convertPatternFieldRef(PRexPatternFieldRef rexPatternFieldRef) {
    Preconditions.checkNotNull(rexPatternFieldRef);

    return new RexPatternFieldRef(
        rexPatternFieldRef.getAlpha(),
        rexPatternFieldRef.getRexInputRef().getIndex(),
        this.types.fromProto(rexPatternFieldRef.getRexInputRef().getDataType()));
  }
}
