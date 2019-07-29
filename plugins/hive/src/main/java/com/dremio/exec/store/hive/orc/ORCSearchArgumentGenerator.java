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
package com.dremio.exec.store.hive.orc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Type;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.ColumnStatistics;

import com.dremio.common.collections.Tuple;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.hive.proto.HiveReaderProto;
import com.google.common.base.Preconditions;

/**
 * Convert the predicate ({@link RexNode}) that can be pushed into ORC reader as {@link SearchArgument}.
 * Input predicate must contain only expressions that can be pushed into ORC reader. It should be the output of
 * {@link ORCFindRelevantFilters}.
 */
class ORCSearchArgumentGenerator extends RexVisitorImpl<Object> {
  private final SearchArgument.Builder sargBuilder;
  private final List<String> columnNames;
  private final List<HiveReaderProto.ColumnInfo> columnInfos;

  ORCSearchArgumentGenerator(final List<String> columnNames, List<HiveReaderProto.ColumnInfo> columnInfos) {
    super(true);
    this.columnNames = columnNames;
    this.columnInfos = columnInfos;
    sargBuilder = SearchArgumentFactory.newBuilder();
    sargBuilder.startAnd();
  }

  SearchArgument get() {
    sargBuilder.end();
    return sargBuilder.build();
  }

  @Override
  public Object visitInputRef(RexInputRef inputRef) {
    return columnNames.get(inputRef.getIndex());
  }

  @Override
  public Object visitLocalRef(RexLocalRef localRef) {
    return null;
  }

  @Override
  public Object visitLiteral(RexLiteral literal) {
    if (RexToExpr.isLiteralNull(literal)) {
      throw new IllegalArgumentException("this shouldn't be part of the input expression: " + literal);
    }

    /**
     * Refer {@link org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl#getMin(ColumnStatistics)}
     * for literal type for given column type
     */
    switch (literal.getType().getSqlTypeName()) {
      case VARCHAR:
      case CHAR:
        return Tuple.<Object, Type>of(((NlsString)literal.getValue()).getValue(), Type.STRING);
      case INTEGER:
        return Tuple.<Object, Type>of(getLong(literal), Type.LONG);
      case BIGINT:
        return  Tuple.<Object, Type>of(getLong(literal), Type.LONG);
      case FLOAT:
        return  Tuple.of(getDouble(literal), Type.FLOAT);
      case DOUBLE:
        return  Tuple.of(getDouble(literal), Type.FLOAT);
      case DECIMAL:
        return  Tuple.of(getDecimal(literal), Type.DECIMAL);
      case DATE:
        // In ORC filter evaluation values are read from file as long and converted to Date in similar way,
        // so the timezone shouldn't be a problem as the input to both here and in ORC reader
        // is millisSinceEpoch in UTC timezone. When this filter is converted to string (for testing purposes),
        // we could see different values depending upon the timezone of JVM
        return Tuple.of(new Date(literal.getValueAs(DateString.class).getMillisSinceEpoch()), Type.DATE);
      case TIMESTAMP:
        // In ORC filter evaluation values are read from file as long and converted to Timestamp in similar way,
        // so the timezone shouldn't be a problem as the input to both here and in ORC reader
        // is millisSinceEpoch in UTC timezone. When this filter is converted to string (for testing purposes),
        // we could see different values depending upon the timezone of JVM
        return Tuple.of(new Timestamp(literal.getValueAs(Long.class)), Type.TIMESTAMP);
      case BOOLEAN:
        return Tuple.of(RexLiteral.booleanValue(literal), Type.BOOLEAN);
      default:
        throw new IllegalArgumentException("this shouldn't be part of the input expression: " + literal);
    }
  }

  private static long getLong(RexLiteral literal) {
    return ((BigDecimal) literal.getValue()).setScale(0, BigDecimal.ROUND_HALF_UP).longValue();
  }

  private static double getDouble(RexLiteral literal) {
    return ((BigDecimal) literal.getValue()).doubleValue();
  }

  private static HiveDecimalWritable getDecimal(RexLiteral literal) {
    return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) literal.getValue()));
  }

  @Override
  public Object visitOver(RexOver over) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + over);
  }

  @Override
  public Object visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + correlVariable);
  }

  @Override
  public Object visitCall(RexCall call) {
    switch (call.getKind()) {
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case EQUALS:
      case NOT_EQUALS: {
        final List<RexNode> children = call.getOperands();
        final RexNode child1 = children.get(0);
        final RexNode child2 = children.get(1);
        boolean reversed = false;
        if (!(child1 instanceof RexInputRef && child2 instanceof RexLiteral) &&
            !(child1 instanceof RexLiteral && child2 instanceof RexInputRef)) {
          throw new IllegalArgumentException("this shouldn't be part of the input expression: " + call);
        }
        int colIndex = -1;
        final String col;
        Tuple<Object, Type> literalPair;
        if (child1 instanceof RexInputRef) {
          colIndex = ((RexInputRef) child1).getIndex();
          col = (String) child1.accept(this);
          literalPair = (Tuple<Object, Type>) child2.accept(this);
        } else {
          reversed = true;
          colIndex = ((RexInputRef) child2).getIndex();
          col = (String) child1.accept(this);
          literalPair = (Tuple<Object, Type>) child1.accept(this);
        }

        if (literalPair.second == Type.STRING && !columnInfos.isEmpty()) {
          final HiveReaderProto.ColumnInfo columnInfo = columnInfos.get(colIndex);
          if (columnInfo.getPrimitiveType() == HiveReaderProto.HivePrimitiveType.CHAR) {
            final int fixedCharLength = columnInfo.getPrecision();
            final String paddedPredicate = HiveBaseChar.getPaddedValue((String)literalPair.first, fixedCharLength);
            literalPair = Tuple.<Object, Type>of(paddedPredicate, Type.STRING);
          }
        }

        // DX-17230: Hive converts min max from ORC statistics metadata to predicate type and then compares
        // So, if predicate type is LONG, and min is -0.1, min gets converted to 0
        // Hence, setting predicate type correctly based on column type
        if (!columnInfos.isEmpty()) {
          final HiveReaderProto.ColumnInfo columnInfo = columnInfos.get(colIndex);
          Type literalType = literalPair.second;
          Object literalValue = literalPair.first;
          if (literalValue.getClass() == Long.class && columnInfo.getPrimitiveType() != null) {
            switch (columnInfo.getPrimitiveType().getNumber()) {
              case HiveReaderProto.HivePrimitiveType.DECIMAL_VALUE:
                literalType = Type.DECIMAL;
                literalValue = new HiveDecimalWritable(HiveDecimal.create((Long)literalValue));
                break;
              case HiveReaderProto.HivePrimitiveType.DOUBLE_VALUE:
              case HiveReaderProto.HivePrimitiveType.FLOAT_VALUE:
                literalType = Type.FLOAT;
                literalValue = new Double((Long)literalValue);
                break;
              default:
                break;
            }
            literalPair = Tuple.<Object, Type>of(literalValue, literalType);
          }
          else if (literalValue.getClass() == HiveDecimalWritable.class && columnInfo.getPrimitiveType() != null) {
            switch (columnInfo.getPrimitiveType().getNumber()) {
              case HiveReaderProto.HivePrimitiveType.DOUBLE_VALUE:
              case HiveReaderProto.HivePrimitiveType.FLOAT_VALUE:
                literalType = Type.FLOAT;
                literalValue = ((HiveDecimalWritable)literalValue).getHiveDecimal().doubleValue();
                literalPair = Tuple.<Object, Type>of(literalValue, literalType);
                break;
              default:
                break;
            }
          }
        }

        switch (call.getKind()) {
          case LESS_THAN:
            if (reversed) {
              // "<" --(reversed args)--> ">=" ---(rewrite in terms of "!", "<" and "<=")--> "!(<)"
              sargBuilder.startNot();
              sargBuilder.lessThan(col, literalPair.second, literalPair.first);
              sargBuilder.end();
            } else {
              sargBuilder.lessThan(col, literalPair.second, literalPair.first);
            }
            return null;
          case LESS_THAN_OR_EQUAL:
            if (reversed) {
              // "<=" --(reversed args)--> ">" ---(rewrite in terms of "!", "<" and "<=")--> "!(<=)"
              sargBuilder.startNot();
              sargBuilder.lessThanEquals(col, literalPair.second, literalPair.first);
              sargBuilder.end();
            } else {
              sargBuilder.lessThanEquals(col, literalPair.second, literalPair.first);
            }
            return null;
          case GREATER_THAN:
            if (reversed) {
              // ">" --(reversed args)--> "<=" ---(rewrite in terms of "!", "<" and "<=")--> "<="
              sargBuilder.lessThanEquals(col, literalPair.second, literalPair.first);
            } else {
              // ">" ---(write in "<" or "<=")--> "!(<=)"
              sargBuilder.startNot();
              sargBuilder.lessThanEquals(col, literalPair.second, literalPair.first);
              sargBuilder.end();
            }
            return null;
          case GREATER_THAN_OR_EQUAL:
            if (reversed) {
              // ">=" --(reversed args)--> "<" ---(rewrite in terms of "!", "<" and "<=")--> "<"
              sargBuilder.lessThan(col, literalPair.second, literalPair.first);
            } else {
              // ">=" ---(write in "<" or "<=")--> "!(<)"
              sargBuilder.startNot();
              sargBuilder.lessThan(col, literalPair.second, literalPair.first);
              sargBuilder.end();
            }
            return null;
          case EQUALS:
            sargBuilder.equals(col, literalPair.second, literalPair.first);
            return null;
          case NOT_EQUALS:
            sargBuilder.startNot();
            sargBuilder.equals(col, literalPair.second, literalPair.first);
            sargBuilder.end();
            return null;
        }
        throw new IllegalArgumentException("this shouldn't be part of the input expression: " + call);
      }

      case IS_NULL:
      case IS_NOT_NULL: {
        final List<RexNode> children = call.getOperands();
        final RexNode child1 = children.get(0);
        final Object evalChild = child1.accept(this);
        if (evalChild == null || !(evalChild instanceof String)) {
          throw new IllegalArgumentException("this shouldn't be part of the input expression: " + call);
        }

        if (call.getKind() == SqlKind.IS_NULL) {
          sargBuilder.isNull((String) evalChild,
              /* not used, just pass a non-null to avoid NPE in hashCode() */ Type.LONG);
        } else {
          // "not null" ----(write in "not" and "is null")--> "not (is null)"
          sargBuilder.startNot();
          sargBuilder.isNull((String) evalChild,
              /* not used, just pass a non-null to avoid NPE in hashCode() */ Type.LONG);
          sargBuilder.end();
        }
        return null;
      }

      case NOT: {
        sargBuilder.startNot();
        call.getOperands().get(0).accept(this);
        sargBuilder.end();
        return null;
      }

      case AND: {
        sargBuilder.startAnd();
        for(RexNode child : call.getOperands()) {
          child.accept(this);
        }
        sargBuilder.end();
        return null;
      }

      case OR: {
        sargBuilder.startOr();
        for(RexNode child : call.getOperands()) {
          child.accept(this);
        }
        sargBuilder.end();
        return null;
      }

      case IN: {
        Preconditions.checkState(call.getOperands().size() >= 1, "Expected IN list to contain at least one element");
        // Operands are a list of EQUAL calls, one for each IN list value
        Tuple<Object, Type>[] inList = new Tuple[call.getOperands().size()];
        int i = 0;
        for(RexNode child : call.getOperands()) {
          Preconditions.checkState(child.getKind() == SqlKind.EQUALS);
          RexNode val = ((RexCall) child).getOperands().get(1);
          inList[i] = (Tuple<Object, Type>) val.accept(this);
          i++;
        }
        // find column name
        RexNode col = ((RexCall) call.getOperands().get(0)).getOperands().get(0);
        sargBuilder.in((String)col.accept(this), inList[0].second, Arrays.stream(inList).map(x -> x.first).toArray());
        return null;
      }

      default:
        throw new IllegalArgumentException("this shouldn't be part of the input expression: " + call);
    }
  }

  @Override
  public Object visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + dynamicParam);
  }

  @Override
  public Object visitRangeRef(RexRangeRef rangeRef) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + rangeRef);
  }

  @Override
  public Object visitFieldAccess(RexFieldAccess fieldAccess) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + fieldAccess);
  }

  @Override
  public Object visitSubQuery(RexSubQuery subQuery) {
    throw new IllegalArgumentException("this shouldn't be part of the input expression: " + subQuery);
  }
}
