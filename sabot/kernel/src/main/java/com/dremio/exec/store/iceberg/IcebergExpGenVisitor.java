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
package com.dremio.exec.store.iceberg;

import java.math.BigDecimal;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import com.dremio.common.exceptions.UserException;

public class IcebergExpGenVisitor extends RexVisitorImpl<Expression> {

    private final List<String> fieldNames;

    IcebergExpGenVisitor(RelDataType rowType) {
        super(true);
        this.fieldNames = rowType.getFieldNames();
    }

    @Override
    public Expression visitInputRef(RexInputRef inputRef) {
        return Expressions.alwaysTrue();
    }

    @Override
    public Expression visitLiteral(RexLiteral literal) {
        return Expressions.alwaysTrue();
    }

    @Override
    public Expression visitCall(RexCall call) {
        final RexNode arg1 = call.getOperands().get(0);
        final RexNode arg2 = call.getOperands().get(1);
        RexInputRef inputRef = null;
        RexNode other = null;
        boolean inputFirst = false;
        if ((arg1 instanceof RexInputRef) && (arg2 instanceof RexLiteral)) {
            inputRef = (RexInputRef) arg1;
            other = arg2;
            inputFirst = true;
        } else if ((arg2 instanceof RexInputRef) && (arg1 instanceof RexLiteral)) {
            inputRef = (RexInputRef) arg2;
            other = arg1;
        }
        if (inputRef != null && other != null) {
            Object val = getValueAsInputRef(inputRef, (RexLiteral) other);
            switch (call.getOperator().getKind()) {
                case LESS_THAN:
                    return inputFirst? Expressions.lessThan(fieldNames.get(inputRef.getIndex()), val) : Expressions.greaterThan(fieldNames.get(inputRef.getIndex()), val);
                case LESS_THAN_OR_EQUAL:
                    return inputFirst? Expressions.lessThanOrEqual(fieldNames.get(inputRef.getIndex()), val) : Expressions.greaterThanOrEqual(fieldNames.get(inputRef.getIndex()), val);
                case GREATER_THAN:
                    return inputFirst? Expressions.greaterThan(fieldNames.get(inputRef.getIndex()), val) : Expressions.lessThan(fieldNames.get(inputRef.getIndex()), val);
                case GREATER_THAN_OR_EQUAL:
                    return inputFirst? Expressions.greaterThanOrEqual(fieldNames.get(inputRef.getIndex()), val) : Expressions.lessThanOrEqual(fieldNames.get(inputRef.getIndex()), val);
                case EQUALS:
                    return Expressions.equal(fieldNames.get(inputRef.getIndex()), val);
                case NOT_EQUALS:
                    return Expressions.notEqual(fieldNames.get(inputRef.getIndex()), val);
                default:
                    throw UserException.validationError().message("Not a valid expression to convert into iceberg expression")
                            .buildSilently();
            }
        } else {
            boolean isAND = false;
            switch (call.getOperator().getKind()) {
                case AND:
                    isAND = true;
                case OR:
                    List<RexNode> nodeList = call.getOperands();
                    Expression left = nodeList.get(0).accept(this);
                    Expression right = nodeList.get(1).accept(this);
                    if(isAND) {
                        return Expressions.and(left, right);
                    } else {
                        return Expressions.or(left, right);
                    }
                default:
                    throw UserException.validationError().message("Not a valid expression to convert into iceberg expression")
                            .buildSilently();
            }
        }
    }

    private Comparable getValueAsInputRef(RexInputRef inputRef, RexLiteral literal) {
        SqlTypeName sqlTypeName = inputRef.getType().getSqlTypeName();
        switch (sqlTypeName) {
            case BOOLEAN:
                return literal.getValueAs(Boolean.class);
            case INTEGER:
            case DATE:
            case TIME:
                return literal.getValueAs(Integer.class);
            case BIGINT:
                return literal.getValueAs(Long.class);
            case TIMESTAMP:
                return literal.getValueAs(Long.class) * 1000;
            case FLOAT:
                return literal.getValueAs(Float.class);
            case DOUBLE:
                return literal.getValueAs(Double.class);
            case VARCHAR:
                return literal.getValueAs(String.class);
            case DECIMAL:
                return literal.getValueAs(BigDecimal.class);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + sqlTypeName);
        }
    }
}

