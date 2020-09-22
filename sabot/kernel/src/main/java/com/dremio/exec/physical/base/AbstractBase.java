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
package com.dremio.exec.physical.base;

import java.util.Collections;
import java.util.Set;

import com.dremio.common.graph.GraphVisitor;
import com.google.common.base.Preconditions;

public abstract class AbstractBase implements PhysicalOperator {
  protected final OpProps props;

  public AbstractBase(OpProps props) {
    Preconditions.checkNotNull(props, String.format("Null props."));
    this.props = props;
  }

  @Override
  public OpProps getProps() {
    return props;
  }

  @Override
  public final void setId(int id) {
    PhysicalOperator.super.setId(id);
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
  public Set<Integer> getExtCommunicableMajorFragments() {
    return Collections.emptySet();
  }
}
