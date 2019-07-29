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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import com.dremio.plugins.elastic.planning.rules.PredicateAnalyzer;
import com.dremio.plugins.elastic.planning.rules.SchemaField.NullReference;
import com.google.common.collect.Iterables;


class CompareFunction extends ElasticFunction {

  private final Type type;

  enum Type {
    LT, GT, LTE, GTE, EQ, NEQ
  }

  public CompareFunction(String commonName, String elasticName, Type type){
    super(commonName, elasticName);
    this.type = type;
  }

  public CompareFunction(String commonName, Type type){
    this(commonName, commonName, type);
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 2);
    PredicateAnalyzer.checkForIncompatibleDateTimeOperands(call);

    RexNode o1 = call.getOperands().get(0);
    RexNode o2 = call.getOperands().get(1);

    FunctionRender op1 = o1.accept(renderer.getVisitor());
    FunctionRender op2 = o2.accept(renderer.getVisitor());

    boolean isTime1 = isTemporal(o1.getType());
    boolean isTime2 = isTemporal(o2.getType());

    if(isTime1 != isTime2){
      throw new RuntimeException("Can't do comparison between a date and a non-date field.");
    }

    // we need special handling in painless for temporal types and comparison other than equality/inequality.
    if(renderer.isUsingPainless() && isTime1 && type != Type.EQ && type != Type.NEQ){
      return handlePainlessTimeComparison(op1, op2);
    }

    String script = String.format("( %s %s %s )", op1.getScript(), elasticName, op2.getScript());
    return new FunctionRender(script, Iterables.concat(op1.getNulls(), op2.getNulls()));
  }

  private FunctionRender handlePainlessTimeComparison(FunctionRender op1, FunctionRender op2){
    final Iterable<NullReference> nulls = nulls(op1, op2);

    switch(type){
    case GT: {
      String script = String.format("(%s).isAfter(%s)", op1.getScript(), op2.getScript());
      return new FunctionRender(script, nulls);
    }

    case GTE: {
      String script = String.format("(%s.equals(%s) || (%s).isAfter(%s))", op1.getScript(), op2.getScript(), op1.getScript(), op2.getScript());
      return new FunctionRender(script, nulls);
    }

    case LT: {
      String script = String.format("(%s).isBefore(%s)", op1.getScript(), op2.getScript());
      return new FunctionRender(script, nulls);
    }

    case LTE: {
      String script = String.format("(%s.equals(%s) || (%s).isBefore(%s))", op1.getScript(), op2.getScript(), op1.getScript(), op2.getScript());
      return new FunctionRender(script, nulls);
    }

    default:
      throw new IllegalStateException(type.name());

    }
  }

  private boolean isTemporal(RelDataType rt){
    switch(rt.getSqlTypeName()){
    case DATE:
    case TIME:
    case TIMESTAMP:
      return true;
    default:
      return false;
    }
  }

}
