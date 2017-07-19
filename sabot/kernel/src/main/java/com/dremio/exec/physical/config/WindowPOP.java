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

package com.dremio.exec.physical.config;

import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Order;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.sabot.op.windowframe.WindowFunction;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rex.RexWindowBound;

import java.util.ArrayList;
import java.util.List;

@JsonTypeName("window")
public class WindowPOP extends AbstractSingle {

  private final List<NamedExpression> withins;
  private final List<NamedExpression> aggregations;
  private final List<Order.Ordering> orderings;
  private final boolean frameUnitsRows;
  private final Bound start;
  private final Bound end;

  public WindowPOP(@JsonProperty("child") PhysicalOperator child,
                   @JsonProperty("within") List<NamedExpression> withins,
                   @JsonProperty("aggregations") List<NamedExpression> aggregations,
                   @JsonProperty("orderings") List<Order.Ordering> orderings,
                   @JsonProperty("frameUnitsRows") boolean frameUnitsRows,
                   @JsonProperty("start") Bound start,
                   @JsonProperty("end") Bound end) {
    super(child);
    this.withins = withins;
    this.aggregations = aggregations;
    this.orderings = orderings;
    this.frameUnitsRows = frameUnitsRows;
    this.start = start;
    this.end = end;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new WindowPOP(child, withins, aggregations, orderings, frameUnitsRows, start, end);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitWindowFrame(this, value);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.WINDOW_VALUE;
  }

  public Bound getStart() {
    return start;
  }

  public Bound getEnd() {
    return end;
  }

  public List<NamedExpression> getAggregations() {
    return aggregations;
  }

  public List<NamedExpression> getWithins() {
    return withins;
  }

  public List<Order.Ordering> getOrderings() {
    return orderings;
  }

  public boolean isFrameUnitsRows() {
    return frameUnitsRows;
  }

  @JsonTypeName("windowBound")
  public static class Bound {
    private final boolean unbounded;
    private final long offset;

    public Bound(@JsonProperty("unbounded") boolean unbounded, @JsonProperty("offset") long offset) {
      this.unbounded = unbounded;
      this.offset = offset;
    }

    public boolean isUnbounded() {
      return unbounded;
    }

    @JsonIgnore
    public boolean isCurrent() {
      return offset == 0;
    }

    public long getOffset() {
      return offset;
    }
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    final BatchSchema childSchema = child.getSchema(context);
    List<NamedExpression> exprs = new ArrayList<>();
    for(Field f : childSchema){
      exprs.add(new NamedExpression(new FieldReference(f.getName()), new FieldReference(f.getName())));
    }
    SchemaBuilder schemaBuilder = ExpressionTreeMaterializer.materializeFields(exprs, childSchema, context)
            .setSelectionVectorMode(childSchema.getSelectionVectorMode());
    try (ErrorCollector collector = new ErrorCollectorImpl()) {
      for (NamedExpression expr : aggregations) {
        WindowFunction func = WindowFunction.fromExpression(expr);
        schemaBuilder.addField(func.materialize(expr, childSchema, collector, context));
      }
    }
    return schemaBuilder.build();
  }

  public static Bound newBound(RexWindowBound windowBound) {
    return new Bound(windowBound.isUnbounded(), windowBound.isCurrentRow() ? 0 : Long.MIN_VALUE); //TODO: Get offset to work
  }
}
