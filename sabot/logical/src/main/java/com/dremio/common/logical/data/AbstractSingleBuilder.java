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
package com.dremio.common.logical.data;

import com.google.common.base.Preconditions;

public abstract class AbstractSingleBuilder<T extends SingleInputOperator, X extends AbstractSingleBuilder<T, X>> extends AbstractBuilder<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSingleBuilder.class);

  private LogicalOperator input;

  @Override
  public final T build(){
    Preconditions.checkNotNull(input);
    T out = internalBuild();
    out.setInput(input);
    return out;
  }

  public X setInput(LogicalOperator input){
    this.input = input;
    return (X) this;
  }

  public abstract T internalBuild();

}
