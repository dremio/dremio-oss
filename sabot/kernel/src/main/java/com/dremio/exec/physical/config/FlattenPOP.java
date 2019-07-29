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

import java.util.Iterator;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractSingle;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.options.Options;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;

@Options
@JsonTypeName("flatten")
public class FlattenPOP extends AbstractSingle {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FlattenPOP.class);

  private final SchemaPath column;

  @JsonCreator
  public FlattenPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("column") SchemaPath column
      ) {
    super(props, child);
    this.column = column;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.singletonIterator(child);
  }

  public SchemaPath getColumn() {
    return column;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitFlatten(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new FlattenPOP(props, child, column);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FLATTEN_VALUE;
  }

}
