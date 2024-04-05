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

package com.dremio.exec.physical.config;

import com.dremio.common.logical.data.NamedExpression;
import com.dremio.common.logical.data.Order;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import org.apache.calcite.rex.RexWindowBound;

@JsonTypeName("window")
public class WindowPOP extends AbstractSingle {

  private final List<NamedExpression> withins;
  private final List<NamedExpression> aggregations;
  private final List<Order.Ordering> orderings;
  private final boolean frameUnitsRows;
  private final Bound lowerBound;
  private final Bound upperBound;

  @JsonCreator
  public WindowPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("withins") List<NamedExpression> withins,
      @JsonProperty("aggregations") List<NamedExpression> aggregations,
      @JsonProperty("orderings") List<Ordering> orderings,
      @JsonProperty("frameUnitsRows") boolean frameUnitsRows,
      @JsonProperty("lowerBound") Bound lowerBound,
      @JsonProperty("upperBound") Bound upperBound) {
    super(props, child);
    this.withins = withins;
    this.aggregations = aggregations;
    this.orderings = orderings;
    this.frameUnitsRows = frameUnitsRows;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new WindowPOP(
        props, child, withins, aggregations, orderings, frameUnitsRows, lowerBound, upperBound);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitWindowFrame(this, value);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.WINDOW_VALUE;
  }

  public Bound getLowerBound() {
    return lowerBound;
  }

  public Bound getUpperBound() {
    return upperBound;
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

  public static class Bound {
    private final boolean unbounded;
    private final int offset;
    private final BoundType type;

    @JsonCreator
    public Bound(
        @JsonProperty("unbounded") boolean unbounded,
        @JsonProperty("offset") int offset,
        @JsonProperty("type") BoundType type) {
      this.unbounded = unbounded;
      this.offset = offset;
      this.type = type;
    }

    public boolean isUnbounded() {
      return unbounded;
    }

    public int getOffset() {
      return offset;
    }

    public BoundType getType() {
      return type;
    }
  }

  public enum BoundType {
    @JsonProperty("PRECEDING")
    PRECEDING,
    @JsonProperty("FOLLOWING")
    FOLLOWING,
    @JsonProperty("CURRENT_ROW")
    CURRENT_ROW;

    public static BoundType fromRexWindowBound(RexWindowBound bound) {
      if (bound.isCurrentRow()) {
        return CURRENT_ROW;
      } else if (bound.isFollowing()) {
        return FOLLOWING;
      } else {
        return PRECEDING;
      }
    }
  }

  public static Bound newBound(RexWindowBound windowBound, int offset) {
    return new Bound(
        windowBound.isUnbounded(),
        windowBound.isCurrentRow() ? 0 : offset,
        BoundType.fromRexWindowBound(windowBound));
  }
}
