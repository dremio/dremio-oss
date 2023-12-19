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
package com.dremio.exec.expr.fn.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

public final class ArrayConcat {
  @FunctionTemplate(
    names = {"array_concat", "array_cat"},
    derivation = ArrayConcat.ArrayConcatOutputDerivation.class,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static final class ArrayConcatSimpleFunction implements SimpleFunction {
    @Param FieldReader arrReader1;
    @Param FieldReader arrReader2;
    @Output BaseWriter.ComplexWriter writer;
    @Inject FunctionErrorContext errorContext;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      com.dremio.exec.expr.fn.impl.ArrayConcatUtility.concat(arrReader1, arrReader2, writer, errorContext);
    }
  }

  public static final class ArrayConcatOutputDerivation implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 2);
      return args.get(0).getCompleteType();
    }
  }
}
