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
package com.dremio.exec.store.parquet;

import java.util.Objects;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

public class ParquetFilterCondition {

  public static final Function<ParquetFilterCondition, String> EXTRACT_COLUMN_NAME =
      ((condition) -> condition.getPath().getRootSegment().getPath());

  private static final Set<SqlKind> supportedKinds = ImmutableSet.of(SqlKind.INPUT_REF, SqlKind.FIELD_ACCESS);

  private final SchemaPath path;
  private final ParquetFilterIface filter;
  private final LogicalExpression expr;
  private final int sort;
  private boolean filterChanged = false;

  @JsonCreator
  public ParquetFilterCondition(@JsonProperty("path") SchemaPath path, @JsonProperty("filter") ParquetFilterIface filter, @JsonProperty("expr") LogicalExpression expr, @JsonProperty("sort") int sort) {
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

  @JsonIgnore
  public boolean isModifiedForPushdown() {
    return filterChanged;
  }

  @JsonIgnore
  public void setFilterModifiedForPushdown(boolean filterChanged) {
    this.filterChanged = filterChanged;
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
    ParquetFilterCondition other = (ParquetFilterCondition) obj;
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
    private final RexNode inputRef;
    private final String field;
    private final SchemaPath schemaPath;

    public FilterProperties(RexCall node,
        RelDataType incomingRowType,
        RexNode inputRef,
        SqlKind kind,
        RexLiteral literal) {
      this.node = node;
      this.kind = validSqlKind(kind);
      this.literal = literal;
      this.inputRef = inputRef;

      schemaPath = rexToSchemaPath(inputRef, incomingRowType);
      if (inputRef instanceof RexInputRef) {
        field = rexToField(inputRef, incomingRowType);
      } else {
        field = null;
      }
      rangeType = toRangeType(kind);
    }

    public FilterProperties(RexCall node, RelDataType incomingRowType) {
      this.node = node;
      final RexNode left = node.getOperands().get(0);
      final RexNode right = node.getOperands().get(1);

      if(left.getKind() == SqlKind.LITERAL && (supportedKinds.contains(right.getKind()))) {
        inputRef = right;
        literal = (RexLiteral) left;
        kind = mirrorOperation(node.getKind());
      } else if(right.getKind() == SqlKind.LITERAL && (supportedKinds.contains(left.getKind()))) {
        inputRef = left;
        literal = (RexLiteral) right;
        kind = validSqlKind(node.getKind());
      } else {
        throw new IllegalStateException("Couldn't handle " + node + " " + incomingRowType);
      }
      schemaPath = rexToSchemaPath(inputRef, incomingRowType);
      if (inputRef instanceof RexInputRef) {
        field = rexToField(inputRef, incomingRowType);
      } else {
        field = null;
      }
      rangeType = toRangeType(kind);

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

    public SchemaPath getSchemaPath() {
      return schemaPath;
    }

    public RexNode getInputRef(){
      return inputRef;
    }

    /**
     * FilterProperties.equals does not consider FilterProperties.node because RexNode use reference equality and
     * all the fields of node are already unpacked.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FilterProperties that = (FilterProperties) o;
      return kind == that.kind &&
        rangeType == that.rangeType &&
        literal.equals(that.literal) &&
        inputRef.equals(that.inputRef) &&
        field.equals(that.field) &&
        schemaPath.equals(that.schemaPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(kind, rangeType, literal, inputRef, field, schemaPath);
    }

    @Override
    public String toString() {
      return "FilterProperties{" +
        ", kind=" + kind +
        ", rangeType=" + rangeType +
        ", literal=" + literal +
        ", inputRef=" + inputRef +
        ", field='" + field + '\'' +
        ", schemaPath=" + schemaPath +
        '}';
    }
  }

  /**
   * Rewrite b > a to a < b
   * @param sqlKind
   * @return
   */
  public static SqlKind mirrorOperation(SqlKind sqlKind) {
    switch(sqlKind){
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case EQUALS:
      case NOT_EQUALS:
        return sqlKind;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static String rexToField(RexNode input, RelDataType incomingRowType) throws UnsupportedOperationException {
    if (input.getKind() == SqlKind.INPUT_REF) {
      return incomingRowType.getFieldNames().get(((RexInputRef) input).getIndex());
    }
    throw new UnsupportedOperationException("Unsupported rex node " + input + " on rowtype " + incomingRowType);
  }

  public static SchemaPath rexToSchemaPath(RexNode input, RelDataType incomingRowType) throws UnsupportedOperationException {
    if (input.getKind() == SqlKind.INPUT_REF) {
      return new SchemaPath(incomingRowType.getFieldNames().get(((RexInputRef) input).getIndex()));
    }
    if (input.getKind() == SqlKind.FIELD_ACCESS) {
      RexNode referenceExpr = ((RexFieldAccess) input).getReferenceExpr();
      SchemaPath path = rexToSchemaPath(referenceExpr, incomingRowType);
      return path.getChild(((RexFieldAccess) input).getField().getName());
    }
    throw new UnsupportedOperationException("Unsupported rex node " + input + " on rowtype " + incomingRowType);
  }

  private static RangeType toRangeType(SqlKind sqlKind) {
    switch(sqlKind){
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return RangeType.LOWER_RANGE;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return RangeType.UPPER_RANGE;
      default:
        return RangeType.OTHER;
    }
  }

  private static SqlKind validSqlKind(SqlKind sqlKind){
    switch(sqlKind){
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
      case EQUALS:
      case NOT_EQUALS:
        return sqlKind;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
