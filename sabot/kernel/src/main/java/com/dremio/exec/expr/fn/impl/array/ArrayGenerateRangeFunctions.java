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
package com.dremio.exec.expr.fn.impl.array;

import java.util.List;

import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

public class ArrayGenerateRangeFunctions {
  @FunctionTemplate(names = "array_generate_range",  scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = ComplexTypeElement.class)
  public static class ArrayGenerateRange implements SimpleFunction {

    @Param private NullableIntHolder start;
    @Param private NullableIntHolder stop;
    @Output private BaseWriter.ComplexWriter out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (start.isSet == 0 || stop.isSet == 0) {
        return;
      }
      if (start.value > stop.value) {
        return;
      }
      com.dremio.exec.expr.fn.impl.array.ArrayHelper.generateIntList(out, start.value, stop.value, 1);
    }
  }

  @FunctionTemplate(names = "array_generate_range", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = ComplexTypeElement.class)
  public static class ArrayGenerateRangeWithStep implements SimpleFunction {

    @Param
    private NullableIntHolder start;
    @Param
    private NullableIntHolder stop;
    @Param
    private NullableIntHolder step;
    @Output
    private BaseWriter.ComplexWriter out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      if (start.isSet == 0 || stop.isSet == 0) {
        return;
      }
      if (step.value == 0) {
        throw new UnsupportedOperationException("Step should be a positive or negative number.");
      }

      com.dremio.exec.expr.fn.impl.array.ArrayHelper.generateIntList(out, start.value, stop.value, step.value);
    }
  }
  public static class ComplexTypeElement implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 2 || args.size() == 3);
      for (LogicalExpression arg : args) {
        Preconditions.checkArgument(arg.getCompleteType().isInt());
      }
      CompleteType type = args.get(0).getCompleteType();
      return new CompleteType(
        ArrowType.List.INSTANCE, type.toField(org.apache.arrow.vector.complex.ListVector.DATA_VECTOR_NAME)
      );
    }
  }
}
