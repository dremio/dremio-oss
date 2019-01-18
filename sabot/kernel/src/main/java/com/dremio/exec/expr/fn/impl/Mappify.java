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
package com.dremio.exec.expr.fn.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

public class Mappify {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Mappify.class);

  /*
   * The following function can be invoked when we want to convert a map to a repeated map where every
   * with two fields in each entry:
   * key: the name of the field in the original map and
   * value: value of the field
   *
   * For eg, consider the following json file:
   *
   * {"foo": {"obj":1, "bar":10}}
   * {"foo": {"obj":2, "bar":20}}
   *
   * Invoking mappify(foo) would result in the following output
   *
   * [{"key":"obj", "value":1}, {"key":"bar", "value":10}]
   * [{"key":"obj", "value":2}, {"key":"bar", "value":20}]
   *
   * Currently this function only allows
   * simple maps as input
   * scalar value fields
   * value fields need to be of the same data type
   */
  @FunctionTemplate(names = {"mappify", "kvgen"}, isDeterministic = false, derivation = KvGenOutput.class)
  public static class ConvertMapToKeyValuePairs implements SimpleFunction {

    @Param  FieldReader reader;
    @Inject ArrowBuf buffer;
    @Output ComplexWriter writer;
    @Inject FunctionErrorContext errorContext;

    public void setup() {
    }

    public void eval() {
      buffer = com.dremio.exec.expr.fn.impl.MappifyUtility.mappify(reader, writer, buffer, errorContext);
    }
  }

  public static class KvGenOutput implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1);
      CompleteType type = args.get(0).getCompleteType();
      if(!type.isStruct()){
        throw UserException.functionError()
            .message("The kvgen function can only be used when operating against maps. The type you were attempting to apply it to was a %s.",
                type.toString())
            .build(logger);
      }

      List<Field> children = type.getChildren();

      if(children.isEmpty()){
        throw UserException.functionError()
            .message("The kvgen function can only be used when operating against maps. The type you were attempting to apply it to was a %s.",
                type.toString())
            .build(logger);
      }


      CompleteType valueType = CompleteType.fromField(children.get(0));
      for(int i =0; i < children.size(); i++){
        valueType = valueType.merge(CompleteType.fromField(children.get(i)));
      }

      return new CompleteType(
          ArrowType.List.INSTANCE,
          new CompleteType(
              ArrowType.Struct.INSTANCE,
              CompleteType.VARCHAR.toField(MappifyUtility.fieldKey),
              valueType.toField(MappifyUtility.fieldValue))
          .toField("$data$"));
    }
  }
}
