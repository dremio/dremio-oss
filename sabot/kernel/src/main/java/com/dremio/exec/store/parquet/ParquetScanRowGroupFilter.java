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

import static com.dremio.exec.planner.common.MoreRelOptUtil.getInputRewriterFromProjectedFields;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;

/**
 * A parquet scan filter for row group pruning.
 */
@JsonTypeName("ParquetScanRowGroupFilter")
public class ParquetScanRowGroupFilter {

  @JsonIgnore
  private final RexNode rexFilter;
  private final LogicalExpression expr;

  @JsonCreator
  public ParquetScanRowGroupFilter() {
    this.rexFilter = null;
    this.expr = null;
  }

  public ParquetScanRowGroupFilter(RexNode rexFilter, LogicalExpression expr) {
    this.rexFilter = rexFilter;
    this.expr = expr;
  }

  public RexNode getRexFilter() {
    return rexFilter;
  }

  public LogicalExpression getExpr() {
    return expr;
  }

  @Override
  public String toString() {
    return ExpressionStringBuilder.toString(expr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParquetScanRowGroupFilter that = (ParquetScanRowGroupFilter) o;
    return Objects.equal(getRexFilter(), that.getRexFilter());
  }

  public ParquetScanRowGroupFilter applyProjection(List<SchemaPath> projection, RelDataType rowType, RelOptCluster cluster, BatchSchema batchSchema) {
    final PrelUtil.InputRewriter inputRewriter = getInputRewriterFromProjectedFields(projection, rowType, batchSchema, cluster);
    return new ParquetScanRowGroupFilter(getRexFilter().accept(inputRewriter), getExpr());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rexFilter);
  }
}
