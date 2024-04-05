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
import java.util.List;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrayRemoveFunction {

  @FunctionTemplate(
      name = "array_remove",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
      isDeterministic = false,
      derivation = ListWithoutRemovedElements.class)
  public static class ArrayRemove implements SimpleFunction {

    @Param private FieldReader in;

    @Param(constant = true)
    private FieldReader value;

    @Output private BaseWriter.ComplexWriter out;
    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      Object inputValue = value.readObject();
      if (!in.isSet() || in.readObject() == null || inputValue == null) {
        return;
      }
      if (!in.reader().getMinorType().equals(value.getMinorType())) {
        throw new UnsupportedOperationException(
            String.format(
                "List of %s is not comparable with %s",
                in.reader().getMinorType().toString(), value.getMinorType().toString()));
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader =
          (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      listWriter.startList();
      while (listReader.next()) {
        if (listReader.reader().readObject() != null
            && listReader.reader().readObject().equals(inputValue)) {
          continue;
        }
        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(
            listReader.reader(),
            com.dremio.exec.expr.fn.impl.ArrayRemoveFunction.getWriter(
                listReader.reader(), listWriter));
      }
      listWriter.endList();
    }
  }

  public static class ListWithoutRemovedElements implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 2);
      CompleteType type = args.get(0).getCompleteType();
      if (!type.isList()) {
        throw UserException.functionError()
            .message(
                "The 'array_remove' function can only be used when operating against a list. The type you were attempting to apply it to was a %s.",
                type.toString())
            .build();
      }
      Preconditions.checkArgument(type.getChildren().size() == 1);
      return new CompleteType(ArrowType.List.INSTANCE, type.getChildren().get(0));
    }
  }

  public static FieldWriter getWriter(FieldReader reader, ListWriter writer) {
    switch (reader.getMinorType()) {
      case LIST:
      case FIXED_SIZE_LIST:
      case NULL:
        return (FieldWriter) writer.list();
      case LARGELIST:
        throw new UnsupportedOperationException(reader.getMinorType().toString());
      case MAP:
        return (FieldWriter) writer.map(false);
      default:
        return (FieldWriter) writer;
    }
  }
}
