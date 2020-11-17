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
import org.apache.calcite.sql.type.SqlTypeName;


class CastFunction extends ElasticFunction {

  public CastFunction(){
    super("cast", "cast");
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 1);
    RexNode input = call.getOperands().get(0);
    FunctionRender op1 = input.accept(renderer.getVisitor());
    return new FunctionRender(getCastScript(input.getType(), call.getType(), op1.getScript(), renderer.isUsingPainless()), op1.getNulls());
  }

  private String getCastScript(RelDataType inputType, RelDataType targetType, String inputScript, boolean isPainless){
    final SqlTypeName inputTypeName = inputType.getSqlTypeName();
    final SqlTypeName targetTypeName = targetType.getSqlTypeName();
    final int targetPrecision = targetType.getPrecision();

    switch (inputTypeName) {
      case VARCHAR:
        switch (targetTypeName) {
          case INTEGER:
            return "Integer.parseInt(" + inputScript + ")";
          case DOUBLE:
            return "Double.parseDouble(" + inputScript + ")";
          case FLOAT:
            return "Float.parseFloat(" + inputScript + ")";
          case BIGINT:
            return "Long.parseLong(" + inputScript + ")";
          case BOOLEAN:
            return "(" + inputScript + ").toLowerCase().equals('true') ? true : false";
          case ANY:
          case ROW:
          case STRUCTURED:
          case ARRAY:
            return inputScript;
          default:
            throw new RuntimeException("Cannot push down anything but trivial casts");
        }
      case DOUBLE:
      case FLOAT:
        switch (targetTypeName) {
          case INTEGER:
            return String.format("(int)(%s).doubleValue()", inputScript);
          case BIGINT:
            return String.format("(long)(%s).doubleValue()", inputScript);
          case DOUBLE:
            return String.format("(%s).doubleValue()", inputScript);
          case FLOAT:
            return String.format("(%s).floatValue()", inputScript);
          case VARCHAR:
            int length = targetPrecision;
            if(length < 20){
              // double is 18, float is 10, int64 is 20.

              // Painless doesn't support dynamic dispatch. Java 7 doesn't support Integer.min()
              if(isPainless){
                return String.format("Double.toString(%s).substring(0, Integer.min(%d, Double.toString(%s).length()))", inputScript, length, inputScript);
              } else {
                return String.format("Double.toString(%s).substring(0, Math.min(%d, Double.toString(%s).length()))", inputScript, length, inputScript);
              }
            } else {
              return String.format("Double.toString(%s)", inputScript);
            }
          case ANY:
          case ROW:
          case STRUCTURED:
          case ARRAY:
            return inputScript;
          default:
            throw new RuntimeException("Cannot push down anything but trivial casts");
        }
      case INTEGER:
      case BIGINT:
        switch (targetTypeName) {
          case INTEGER:
            return String.format("(int)(%s)", inputScript);
          case DOUBLE:
            return String.format("(double)(%s)", inputScript);
          case FLOAT:
            return String.format("((float)(%s))", inputScript);
          case BIGINT:
            return String.format("(long)(%s)", inputScript);
          case VARCHAR:
            int length = targetPrecision;
            if(length < 20){
              // double is 18, float is 10, int64 is 20.

              // Painless doesn't support dynamic dispatch. Java 7 doesn't support Integer.min()
              if(isPainless){
                return String.format("Long.toString(%s).substring(0, Integer.min(%d, Double.toString(%s).length()))", inputScript, length, inputScript);
              }else{
                return String.format("Long.toString(%s).substring(0, Math.min(%d, Double.toString(%s).length()))", inputScript, length, inputScript);
              }
            } else {
              return String.format("Long.toString(%s)", inputScript);
            }
          case ANY:
          case ROW:
          case STRUCTURED:
          case ARRAY:
            return inputScript;
          default:
        }
    }
    throw new RuntimeException("Cannot push down anything but trivial casts");
  }

}
