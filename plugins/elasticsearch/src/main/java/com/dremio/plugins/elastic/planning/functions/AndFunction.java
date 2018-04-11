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
package com.dremio.plugins.elastic.planning.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

class AndFunction extends ElasticFunction {

  public AndFunction(){
    super("and", "&&");
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    List<Iterable<NullReference>> refs = new ArrayList<>();

    List<String> operands = Lists.newArrayListWithCapacity(call.getOperands().size());
    for (RexNode childCall : call.getOperands()) {
      FunctionRender r = childCall.accept(renderer.getVisitor());
      operands.add(r.getScript());
      refs.add(r.getNulls());
    }

    return new FunctionRender("( " + Joiner.on(" " + elasticName + " ").join(operands) + " )", Iterables.concat(refs));
  }

}
