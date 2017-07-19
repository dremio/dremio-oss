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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractMultiple;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("union-all")

public class UnionAll extends AbstractMultiple {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Filter.class);

  @JsonCreator
  public UnionAll(@JsonProperty("children") List<PhysicalOperator> children) {
    super(children);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnion(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new UnionAll(children);
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    List<PhysicalOperator> children = getChildren();
    BatchSchema left = children.get(0).getSchema(context);
    BatchSchema right = children.get(1).getSchema(context);
    if(!right.equalsTypesAndPositions(left)){
      throw UserException.dataReadError()
      .message("Unable to complete query, attempting to union two datasets that have different underlying schemas. Left: %s, Right: %s", left, right)
      .build(logger);
    }
    return left;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.UNION_VALUE;
  }
}
