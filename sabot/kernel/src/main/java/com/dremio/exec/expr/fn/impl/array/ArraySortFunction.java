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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;

public class ArraySortFunction {

  @FunctionTemplate(
      name = "array_sort",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.NULL_IF_NULL,
      derivation = ArraySortOutputDerivation.class)
  public static class ArraySort implements SimpleFunction {
    @Param private FieldReader input;
    @Output private BaseWriter.ComplexWriter output;
    @Inject private ArrowBuf indexBuffer;
    @Workspace private ArrayHelper.ListSorter sorter;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!input.isSet() || input.readObject() == null) {
        return;
      }

      int size = input.size();
      indexBuffer = indexBuffer.reallocIfNeeded(size * 4L);

      org.apache.arrow.vector.complex.reader.FieldReader childReader = input.reader();
      int initialPosition = childReader.getPosition();

      sorter =
          new com.dremio.exec.expr.fn.impl.array.ArrayHelper.ListSorter(
              indexBuffer, childReader, initialPosition);
      sorter.sort(size);

      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter outputWriter =
          output.rootAsList();
      outputWriter.startList();

      for (int i = 0; i < size; i++) {
        childReader.setPosition(initialPosition + sorter.mapIndex(i));
        com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyToList(childReader, outputWriter);
      }

      outputWriter.endList();
    }
  }

  public static final class ArraySortOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);
      return type;
    }
  }
}
