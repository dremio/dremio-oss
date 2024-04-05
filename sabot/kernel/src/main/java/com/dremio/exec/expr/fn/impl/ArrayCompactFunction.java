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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrayCompactFunction {

  @FunctionTemplate(
      name = "array_compact",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
      derivation = ListWithoutRemovedElements.class)
  public static class ArrayCompact implements SimpleFunction {

    @Param private FieldReader in;
    @Output private BaseWriter.ComplexWriter out;
    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        return;
      }
      com.dremio.exec.expr.fn.impl.array.ArrayHelper.removeNullList(in, out);
    }
  }

  public static class ListWithoutRemovedElements implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1);
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);
      return new CompleteType(ArrowType.List.INSTANCE, type.getChildren().get(0));
    }
  }
}
