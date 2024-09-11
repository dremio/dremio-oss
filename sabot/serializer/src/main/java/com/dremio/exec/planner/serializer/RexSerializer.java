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

import com.dremio.exec.planner.sql.SqlFlattenOperator;
import com.dremio.plan.serialization.PBigDecimal;
import com.dremio.plan.serialization.PBoundOption;
import com.dremio.plan.serialization.POptionality;
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
import com.dremio.plan.serialization.PRexVariable;
import com.dremio.plan.serialization.PRexWinAggCall;
import com.dremio.plan.serialization.PRexWindow;
import com.dremio.plan.serialization.PRexWindowBound;
import com.dremio.plan.serialization.PRexWindowBoundBounded;
import com.dremio.plan.serialization.PRexWindowBoundCurrentRow;
import com.dremio.plan.serialization.PRexWindowBoundUnbounded;
import com.dremio.plan.serialization.PSqlAggFunction;
import com.dremio.plan.serialization.PSqlFunction;
import com.dremio.plan.serialization.PSqlFunctionCategory;
import com.dremio.plan.serialization.PSqlIdentifier;
import com.dremio.plan.serialization.PSqlParserPos;
import com.dremio.plan.serialization.PSymbol;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVariable;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

/** Serializer for RexNodes. */
public final class RexSerializer implements RexVisitor<PRexNode> {

  private final RexBuilder rexBuilder;
  private final TypeSerde typeSerializer;
  private final RelSerdeRegistry registry;
  private final List<Any> nodes = new ArrayList<>();
  private final SqlOperatorSerde sqlOperatorSerde;

  public RexSerializer(
      RexBuilder rexBuilder,
      TypeSerde typeSerializer,
      RelSerdeRegistry registry,
      SqlOperatorSerde sqlOperatorSerde) {
    super();
    this.rexBuilder = rexBuilder;
    this.typeSerializer = typeSerializer;
    this.registry = registry;
    this.sqlOperatorSerde = sqlOperatorSerde;
  }

  @Override
  public PRexNode visitInputRef(RexInputRef inputRef) {
    return PRexNode.newBuilder()
        .setRexInputRef(
            PRexInputRef.newBuilder()
                .setDataType(typeSerializer.toProto(inputRef.getType()))
                .setIndex(inputRef.getIndex()))
        .build();
  }

  @Override
  public PRexNode visitLocalRef(RexLocalRef localRef) {
    return PRexNode.newBuilder()
        .setRexLocalRef(
            PRexLocalRef.newBuilder()
                .setDataType(typeSerializer.toProto(localRef.getType()))
                .setIndex(localRef.getIndex()))
        .build();
  }

  @Override
  public PRexNode visitLiteral(RexLiteral literal) {
    PRexLiteral.Builder builder =
        PRexLiteral.newBuilder()
            .setDataType(typeSerializer.toProto(literal.getType()))
            .setTypeName(TypeSerde.toProto(literal.getTypeName()));
    builder = addValue(literal, builder);
    return PRexNode.newBuilder().setRexLiteral(builder).build();
  }

  private PRexLiteral.Builder addValue(RexLiteral literal, PRexLiteral.Builder builder) {
    Object value = literal.getValue3();
    if (value == null) {
      return builder;
    }

    if (value instanceof Calendar) {
      return builder.setLongValue(((Calendar) value).getTimeInMillis());
    }

    if (value instanceof Boolean) {
      return builder.setBooleanValue((Boolean) value);
    }

    if (value instanceof String) {
      return builder.setStringValue((String) value);
    }
    if (value instanceof org.apache.calcite.avatica.util.ByteString) {
      return builder.setBinaryValue(
          ByteString.copyFrom(((org.apache.calcite.avatica.util.ByteString) value).getBytes()));
    }

    if (value instanceof DateString) {
      return builder.setLongValue(((DateString) value).getMillisSinceEpoch());
    }

    if (value instanceof TimeString) {
      return builder.setLongValue(((TimeString) value).getMillisOfDay());
    }

    if (value instanceof TimestampString) {
      return builder.setLongValue(((TimestampString) value).getMillisSinceEpoch());
    }

    if (value instanceof Long) {
      return builder.setLongValue((Long) value);
    }

    if (value instanceof Integer) {
      return builder.setLongValue(((Integer) value).longValue());
    }

    if (value instanceof BigDecimal) {
      BigDecimal bigDecimal = (BigDecimal) value;
      BigInteger unscaledValue = bigDecimal.unscaledValue();
      return builder.setDecimalValue(
          PBigDecimal.newBuilder()
              .setScale(bigDecimal.scale())
              .setTwosComplementValue(ByteString.copyFrom(unscaledValue.toByteArray()))
              .build());
    }

    if (value instanceof NlsString) {
      return builder
          .setStringValue(((NlsString) value).getValue())
          .setCharset(((NlsString) value).getCharsetName())
          .setCollation(typeSerializer.toProto(((NlsString) value).getCollation()));
    }

    if (value instanceof Enum) {
      Enum e = (Enum) value;
      String clazz = e.getClass().getName();
      String name = e.name();
      return builder.setSymbolValue(PSymbol.newBuilder().setClazz(clazz).setName(name).build());
    }

    throw new UnsupportedOperationException(
        String.format("Unhandled serialization for %s", value.getClass().getName()));
  }

  @Override
  public PRexNode visitCall(RexCall call) {
    if (call instanceof RexWinAggCall) {
      return toProto((RexWinAggCall) call);
    }
    PRexCall.Builder builder =
        PRexCall.newBuilder()
            .addAllOperands(
                call.getOperands().stream().map(o -> o.accept(this)).collect(Collectors.toList()))
            .setDataType(typeSerializer.toProto(call.getType()));
    SqlOperator op = call.getOperator();
    builder.setSqlOperator(sqlOperatorSerde.toProto(op));
    if (op instanceof SqlFlattenOperator) {
      builder.setIndex(((SqlFlattenOperator) op).getIndex());
    }
    return PRexNode.newBuilder().setRexCall(builder).build();
  }

  public PRexNode toProto(RexWinAggCall rexWinAggCall) {
    SqlAggFunction sqlAggFunction = (SqlAggFunction) rexWinAggCall.getOperator();

    PRexCall.Builder rexCallBuilder =
        PRexCall.newBuilder()
            .addAllOperands(
                rexWinAggCall.getOperands().stream()
                    .map(o -> o.accept(this))
                    .collect(Collectors.toList()))
            .setDataType(typeSerializer.toProto(rexWinAggCall.getType()))
            .setSqlOperator(sqlOperatorSerde.toProto(sqlAggFunction));

    PSqlAggFunction.Builder sqlAggFunctionBuilder =
        PSqlAggFunction.newBuilder()
            .setSqlFunction(toProto(sqlAggFunction))
            .setRequiresGroupOrder(toProto(sqlAggFunction.requiresGroupOrder()))
            .setRequiresOrder(sqlAggFunction.requiresOrder())
            .setRequiresOver(sqlAggFunction.requiresOver());

    return PRexNode.newBuilder()
        .setRexWindowAggCall(
            PRexWinAggCall.newBuilder()
                .setRexCall(rexCallBuilder)
                .setSqlAggFunction(sqlAggFunctionBuilder)
                .setOrdinal(rexWinAggCall.ordinal)
                .setDistinct(rexWinAggCall.distinct)
                .setIgnoreNulls(rexWinAggCall.ignoreNulls))
        .build();
  }

  private PSqlFunction toProto(SqlAggFunction sqlFunction) {
    PSqlFunction.Builder builder = PSqlFunction.newBuilder();
    if (sqlFunction.getParamTypes() != null && !sqlFunction.getParamTypes().isEmpty()) {
      builder.addAllParamTypes(
          sqlFunction.getParamTypes().stream()
              .map(typeSerializer::toProto)
              .collect(Collectors.toList()));
    }
    builder.setFunctionCatagory(toProto(sqlFunction.getFunctionType()));
    if (sqlFunction.getSqlIdentifier() != null) {
      builder.setSqlIdentifier(toProto(sqlFunction.getSqlIdentifier()));
    }
    return builder.build();
  }

  private PSqlIdentifier toProto(SqlIdentifier sqlIdentifier) {
    List<PSqlParserPos> componentPositions = new ArrayList<>();
    for (int i = 0; i < sqlIdentifier.names.size(); i++) {
      componentPositions.add(toProto(sqlIdentifier.getComponentParserPosition(i)));
    }
    return PSqlIdentifier.newBuilder()
        .setCollation(typeSerializer.toProto(sqlIdentifier.getCollation()))
        .addAllNames(sqlIdentifier.names)
        .addAllComponentPositions(componentPositions)
        .build();
  }

  private PSqlParserPos toProto(SqlParserPos sqlParserPos) {
    return PSqlParserPos.newBuilder()
        .setLineNumber(sqlParserPos.getLineNum())
        .setColumnNumber(sqlParserPos.getColumnNum())
        .setEndLineNumber(sqlParserPos.getEndLineNum())
        .setEndColumnNumber(sqlParserPos.getEndColumnNum())
        .build();
  }

  private POptionality toProto(Optionality optionality) {
    switch (optionality) {
      case OPTIONAL:
        return POptionality.OPTIONAL;
      case MANDATORY:
        return POptionality.MANDATORY;
      case FORBIDDEN:
        return POptionality.FORBIDDEN;
      default:
        return POptionality.IGNORED;
    }
  }

  private PSqlFunctionCategory toProto(SqlFunctionCategory sqlFunctionCategory) {
    switch (sqlFunctionCategory) {
      case STRING:
        return PSqlFunctionCategory.STRING;
      case NUMERIC:
        return PSqlFunctionCategory.NUMERIC;
      case TIMEDATE:
        return PSqlFunctionCategory.TIMEDATE;
      case SYSTEM:
        return PSqlFunctionCategory.SYSTEM;
      case USER_DEFINED_FUNCTION:
        return PSqlFunctionCategory.USER_DEFINED_FUNCTION;
      case USER_DEFINED_PROCEDURE:
        return PSqlFunctionCategory.USER_DEFINED_PROCEDURE;
      case USER_DEFINED_CONSTRUCTOR:
        return PSqlFunctionCategory.USER_DEFINED_CONSTRUCTOR;
      case USER_DEFINED_SPECIFIC_FUNCTION:
        return PSqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION;
      case USER_DEFINED_TABLE_FUNCTION:
        return PSqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
      case USER_DEFINED_TABLE_SPECIFIC_FUNCTION:
        return PSqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION;
      default:
        return PSqlFunctionCategory.MATCH_RECOGNIZE;
    }
  }

  @Override
  public PRexNode visitOver(RexOver over) {
    PRexWindow.Builder builder = PRexWindow.newBuilder();

    RexWindow w = over.getWindow();

    if (w.getLowerBound() != null) {
      builder.setLowerBound(toProto(over.getWindow().getLowerBound()));
    }

    if (w.getUpperBound() != null) {
      builder.setUpperBound(toProto(over.getWindow().getUpperBound()));
    }

    List<PRexFieldCollation> collations =
        w.orderKeys.stream()
            .map(
                rexFieldCollation ->
                    PRexFieldCollation.newBuilder()
                        .setLeft(rexFieldCollation.left.accept(this))
                        .addAllRight(
                            rexFieldCollation.right.stream()
                                .map(this::toProto)
                                .collect(Collectors.toList()))
                        .build())
            .collect(Collectors.toList());

    builder
        .addAllOrderKeys(collations)
        .addAllPartitionKeys(
            w.partitionKeys.stream().map(p -> p.accept(this)).collect(Collectors.toList()))
        .setIsRows(w.isRows());

    return PRexNode.newBuilder()
        .setRexOver(
            PRexOver.newBuilder()
                .addAllOperands(
                    over.operands.stream().map(p -> p.accept(this)).collect(Collectors.toList()))
                .setDataType(typeSerializer.toProto(over.getType()))
                .setSqlOperator(sqlOperatorSerde.toProto(over.getOperator()))
                .setRexWindow(builder))
        .build();
  }

  public PRexWindowBound toProto(RexWindowBound bound) {
    if (bound.isCurrentRow()) {
      return PRexWindowBound.newBuilder()
          .setCurrentRow(PRexWindowBoundCurrentRow.newBuilder())
          .build();
    }

    PRexNode offset = bound.getOffset() == null ? null : bound.getOffset().accept(this);

    if (bound.isUnbounded()) {
      return PRexWindowBound.newBuilder()
          .setUnbounded(
              PRexWindowBoundUnbounded.newBuilder()
                  .setBoundOption(
                      bound.isPreceding() ? PBoundOption.PRECEDING : PBoundOption.FOLLOWING))
          .build();
    }

    return PRexWindowBound.newBuilder()
        .setBounded(
            PRexWindowBoundBounded.newBuilder()
                .setOffset(offset)
                .setBoundOption(
                    bound.isPreceding() ? PBoundOption.PRECEDING : PBoundOption.FOLLOWING))
        .build();
  }

  private PRexFieldCollation.PSortFlag toProto(SqlKind sqlKind) {
    switch (sqlKind) {
      case DESCENDING:
        return PRexFieldCollation.PSortFlag.DESCENDING;
      case NULLS_FIRST:
        return PRexFieldCollation.PSortFlag.NULLS_FIRST;
      case NULLS_LAST:
        return PRexFieldCollation.PSortFlag.NULLS_LAST;
      default:
        throw new UnsupportedOperationException("Unknown type: " + sqlKind);
    }
  }

  @Override
  public PRexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    PRexFieldAccess pRexFieldAccess =
        PRexFieldAccess.newBuilder()
            .setExpression(fieldAccess.getReferenceExpr().accept(this))
            .setField(typeSerializer.toProto(fieldAccess.getField()))
            .build();
    return PRexNode.newBuilder().setRexFieldAccess(pRexFieldAccess).build();
  }

  @Override
  public PRexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
    PRexCorrelVariable pRexCorrelVariable =
        PRexCorrelVariable.newBuilder()
            .setCorrelationId(correlVariable.id.getId())
            .setCorrelationName(correlVariable.getName())
            .setDataType(typeSerializer.toProto(correlVariable.getType()))
            .build();
    return PRexNode.newBuilder().setRexCorrelVariable(pRexCorrelVariable).build();
  }

  @Override
  public PRexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    PRexDynamicParam pRexDynamicParam =
        PRexDynamicParam.newBuilder()
            .setRexVariable(this.visitRexVariable(dynamicParam))
            .setIndex(dynamicParam.getIndex())
            .build();

    return PRexNode.newBuilder().setRexDynamicParam(pRexDynamicParam).build();
  }

  private PRexVariable visitRexVariable(RexVariable rexVariable) {
    return PRexVariable.newBuilder()
        .setType(this.typeSerializer.toProto(rexVariable.getType()))
        .setName(rexVariable.getName())
        .build();
  }

  @Override
  public PRexNode visitRangeRef(RexRangeRef rangeRef) {
    PRexRangeRef pRexRangeRef =
        PRexRangeRef.newBuilder()
            .setType(this.typeSerializer.toProto(rangeRef.getType()))
            .setOffset(rangeRef.getOffset())
            .build();

    return PRexNode.newBuilder().setRexRangeRef(pRexRangeRef).build();
  }

  @Override
  public PRexNode visitSubQuery(RexSubQuery subQuery) {
    PRelList pRelList = RelSerializer.serializeList(registry, subQuery.rel, sqlOperatorSerde);

    PRexSubQuery pRexSubQuery =
        PRexSubQuery.newBuilder()
            .setDataType(typeSerializer.toProto(subQuery.getType()))
            .addAllOperands(
                subQuery.getOperands().stream()
                    .map(o -> o.accept(this))
                    .collect(Collectors.toList()))
            .setSqlOperator(sqlOperatorSerde.toProto(subQuery.getOperator()))
            .addAllDetails(pRelList.getNodeList())
            .build();
    return PRexNode.newBuilder().setRexSubquery(pRexSubQuery).build();
  }

  @Override
  public PRexNode visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PRexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return PRexNode.newBuilder()
        .setRexPatternFieldRef(
            PRexPatternFieldRef.newBuilder()
                .setAlpha(fieldRef.getAlpha())
                .setRexInputRef(
                    PRexInputRef.newBuilder()
                        .setDataType(this.typeSerializer.toProto(fieldRef.getType()))
                        .setIndex(fieldRef.getIndex())
                        .build())
                .build())
        .build();
  }
}
