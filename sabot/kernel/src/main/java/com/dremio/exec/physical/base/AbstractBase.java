/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.physical.base;

import com.dremio.common.graph.GraphVisitor;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;

public abstract class AbstractBase implements PhysicalOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBase.class);

  private final String userName;

  private long initialAllocation = 1000000L;
  private long maxAllocation = Long.MAX_VALUE;
  private int id;
  private double cost;
  private boolean isSingle = false;
  private BatchSchema cachedSchema;

  public AbstractBase() {
    userName = null;
  }

  public AbstractBase(String userName) {
    this.userName = userName;
  }

  public AbstractBase(AbstractBase that) {
    Preconditions.checkNotNull(that, "Unable to clone: source is null.");
    this.userName = that.userName;
  }

  @Override
  public void setAsSingle() {
    isSingle = true;
  }

  @Override
  public boolean isSingle() {
    return isSingle;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    if (this.iterator() == null) {
      throw new IllegalArgumentException("Null iterator for pop." + this);
    }
    for (PhysicalOperator o : this) {
      Preconditions.checkNotNull(o, String.format("Null in iterator for pop %s.", this));
      o.accept(visitor);
    }
    visitor.leave(this);
  }

  @Override
  public final void setOperatorId(int id) {
    this.id = id;
  }

  @Override
  public int getOperatorId() {
    return id;
  }

  @Override
  public long getInitialAllocation() {
    return initialAllocation;
  }

  public void setInitialAllocation(long initialAllocation) {
    this.initialAllocation = initialAllocation;
  }

  @Override
  public double getCost() {
    return cost;
  }

  @Override
  public void setCost(double cost) {
    this.cost = cost;
  }

  @Override
  public long getMaxAllocation() {
    return maxAllocation;
  }

  public void setMaxAllocation(long maxAllocation) {
    this.maxAllocation = maxAllocation;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public final BatchSchema getSchema(FunctionLookupContext context) {
    if (cachedSchema == null) {
      cachedSchema = constructSchema(context);
    }

    return cachedSchema;
  }

  protected abstract BatchSchema constructSchema(FunctionLookupContext context);
}
