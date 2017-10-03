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

import java.util.List;

import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("external-sort")
public class ExternalSort extends Sort {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSort.class);

  @JsonCreator
  public ExternalSort(@JsonProperty("child") PhysicalOperator child, @JsonProperty("orderings") List<Ordering> orderings, @JsonProperty("reverse") boolean reverse) {
    super(child, orderings, reverse);
    setInitialAllocation(20000000);
  }

  @Override
  public List<Ordering> getOrderings() {
    return orderings;
  }

  @Override
  public boolean getReverse() {
    return reverse;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitSort(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    ExternalSort newSort = new ExternalSort(child, orderings, reverse);
    newSort.setMaxAllocation(getMaxAllocation());
    return newSort;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.EXTERNAL_SORT_VALUE;
  }


  @Override
  public void setMaxAllocation(long maxAllocation) {
    super.setMaxAllocation(Math.max(getInitialAllocation(), maxAllocation));
  }

}
