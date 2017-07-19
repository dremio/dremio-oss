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
package com.dremio.exec.store.parquet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;

public class FilterCondition {

  public static final Function<FilterCondition, String> EXTRACT_COLUMN_NAME = new Function<FilterCondition, String>() {
    @Override
    public String apply(FilterCondition condition) {
      return condition.getPath().getRootSegment().getPath();
    }
  };

  private final SchemaPath path;
  private final ParquetFilterIface filter;
  private final LogicalExpression expr;
  private final int sort;

  @JsonCreator
  public FilterCondition(@JsonProperty("path") SchemaPath path, @JsonProperty("filter") ParquetFilterIface filter, @JsonProperty("expr") LogicalExpression expr, @JsonProperty("sort") int sort) {
    super();
    this.path = path;
    this.filter = filter;
    this.expr = expr;
    this.sort = sort;
  }

  public LogicalExpression getExpr(){
    return expr;
  }

  public ParquetFilterIface getFilter() {
    return filter;
  }

  public SchemaPath getPath() {
    return path;
  }

  public int getSort() {
    return sort;
  }

  @Override
  public String toString() {
    return "Filter on " + path + ": " + ExpressionStringBuilder.toString(expr);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FilterCondition other = (FilterCondition) obj;
    if (filter == null) {
      if (other.filter != null) {
        return false;
      }
    } else if (!filter.equals(other.filter)) {
      return false;
    }
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }

    return true;
  }

  public enum RangeType {OTHER, LOWER_RANGE, UPPER_RANGE}
  public static class FilterProperties {

    private final RexCall node;
    private final SqlKind kind;
    private final RangeType rangeType;

    // left arg
    private final RexLiteral literal;
    private final String field;

    public FilterProperties(RexCall node, RelDataType incomingRowType) {
      this.node = node;
      switch(node.getKind()){
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
      case EQUALS:
      case NOT_EQUALS:
        break;
      default:
        throw new UnsupportedOperationException();
      }

      final RexNode left = node.getOperands().get(0);
      final RexNode right = node.getOperands().get(1);

      if(left.getKind() == SqlKind.LITERAL && right.getKind() == SqlKind.INPUT_REF){
        literal = (RexLiteral) left;
        field = incomingRowType.getFieldNames().get(((RexInputRef) right).getIndex());
        switch(node.getKind()){
        case LESS_THAN:
          kind = SqlKind.GREATER_THAN;
          break;

        case GREATER_THAN:
          kind = SqlKind.LESS_THAN;
          break;

        case LESS_THAN_OR_EQUAL:
          kind = SqlKind.GREATER_THAN_OR_EQUAL;
          break;

        case GREATER_THAN_OR_EQUAL:
          kind = SqlKind.LESS_THAN_OR_EQUAL;
          break;

        case EQUALS:
        case NOT_EQUALS:
          kind = node.getKind();
          break;
        default:
          throw new UnsupportedOperationException();
        }
      } else if(right.getKind() == SqlKind.LITERAL && left.getKind() == SqlKind.INPUT_REF) {
        literal = (RexLiteral) right;
        field = incomingRowType.getFieldNames().get(((RexInputRef) left).getIndex());
        kind = node.getKind();
      } else {
        throw new IllegalStateException("Couldn't handle " + node + " " + incomingRowType);
      }

      switch(this.kind){
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        this.rangeType = RangeType.LOWER_RANGE;
        break;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        this.rangeType = RangeType.UPPER_RANGE;
        break;
      default:
        this.rangeType = RangeType.OTHER;

      }
    }

    public RangeType getRangeType(){
      return rangeType;
    }

    public String getOpName(){
      switch(kind){
      case GREATER_THAN:
        return "gt";
      case GREATER_THAN_OR_EQUAL:
        return "gte";
      case LESS_THAN:
        return "lt";
      case LESS_THAN_OR_EQUAL:
        return "lte";
      case EQUALS:
        return "eq";
      default:
        throw new UnsupportedOperationException("Invalid kind " + kind);
      }
    }

    public boolean isOpen(){
      return kind == SqlKind.GREATER_THAN || kind == SqlKind.LESS_THAN;
    }

    public RexCall getNode() {
      return node;
    }

    public SqlKind getKind() {
      return kind;
    }

    public RexLiteral getLiteral() {
      return literal;
    }

    public String getField() {
      return field;
    }

  }
}
