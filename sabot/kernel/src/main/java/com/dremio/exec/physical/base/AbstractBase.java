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
package com.dremio.exec.physical.base;

import com.dremio.common.graph.GraphVisitor;
import com.google.common.base.Preconditions;

public abstract class AbstractBase implements PhysicalOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBase.class);

  private final String userName;

  protected long initialAllocation = 1000000L;
  protected long maxAllocation = Long.MAX_VALUE;
  private int id;
  private double cost;
  private boolean isSingle = false;

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

  public double getCost() {
    return cost;
  }

  public void setCost(double cost) {
    this.cost = cost;
  }

  @Override
  public long getMaxAllocation() {
    return maxAllocation;
  }

  @Override
  public String getUserName() {
    return userName;
  }
}
