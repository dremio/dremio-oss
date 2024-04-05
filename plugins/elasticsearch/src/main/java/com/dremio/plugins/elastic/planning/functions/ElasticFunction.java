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
package com.dremio.plugins.elastic.planning.functions;

import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexCall;

public abstract class ElasticFunction {

  protected static final Iterable<NullReference> EMPTY = ImmutableList.of();

  protected final String dremioName;
  protected final String elasticName;

  public ElasticFunction(String dremioName, String elasticName) {
    this.dremioName = dremioName;
    this.elasticName = elasticName;
  }

  public abstract FunctionRender render(FunctionRenderer renderer, RexCall call);

  protected void checkArity(RexCall call, int num) {
    Preconditions.checkArgument(
        call.getOperands().size() == num,
        "Function operation %s expected %s arguments but received %s.",
        dremioName,
        num,
        call.getOperands().size());
  }

  protected static Iterable<NullReference> nulls(FunctionRender... renders) {
    return FluentIterable.from(renders)
        .transformAndConcat(
            new Function<FunctionRender, Iterable<NullReference>>() {
              @Override
              public Iterable<NullReference> apply(FunctionRender input) {
                return input.getNulls();
              }
            });
  }

  public String getDremioName() {
    return dremioName;
  }

  public String getElasticName() {
    return elasticName;
  }
}
