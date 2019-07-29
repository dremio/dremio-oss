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

import java.util.Set;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.dremio.plugins.elastic.planning.rules.SchemaField.ReferenceType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;


class OrFunction extends ElasticFunction {

  public OrFunction(){
    super("or", "||");
  }

  @Override
  public FunctionRender render(final FunctionRenderer renderer, RexCall call) {
    String script = FluentIterable.from(call.getOperands()).transform(new Function<RexNode, String>(){
      @Override
      public String apply(RexNode input) {
        FunctionRender render = input.accept(renderer.getVisitor());
        String notNullScript = getNotNullScript(render.getNulls());
        if(notNullScript == null){
          return render.getScript();
        }

        return String.format("(%s && %s)", notNullScript, render.getScript());
      }}).join(Joiner.on(" || "));

    return new FunctionRender(script, EMPTY);
  }


  private String getNotNullScript(Iterable<NullReference> nulls){
    Set<NullReference> set = FluentIterable.from(nulls).toSet();
    if(set.isEmpty()){
      return null;
    }

    return FluentIterable.from(set).transform(new Function<NullReference, String>(){

      @Override
      public String apply(NullReference toCheck) {
        if(toCheck.getReferenceType() == ReferenceType.SOURCE){
          return String.format("(%s != null)", toCheck.getValue());
        }else if (toCheck.getReferenceType() == ReferenceType.DOC){
          return String.format("(!%s.empty)", toCheck.getValue());
        }else{
          throw new UnsupportedOperationException("Unknown reference type." + toCheck.getReferenceType());
        }
      }}).join(Joiner.on(" && "));
  }


}
