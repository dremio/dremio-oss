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
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class SubListFunction {

  /**
   * Extract the given range of elements for input list.
   *
   * <p>Parameters: 1. in : input column as field reader 2. offset : 1-based starting element index
   * (inclusive) (negative if the direction is from the end) 3. length : max number of elements from
   * the start
   */
  @FunctionTemplate(
      name = "sublist",
      scope = FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL,
      derivation = SubListOutputDerivation.class)
  public static class SubList implements SimpleFunction {
    @Param private FieldReader in;
    @Param private NullableIntHolder start; // Inclusive
    @Param private NullableIntHolder length; // Max number of elements from start
    @Output private ComplexWriter out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        out.rootAsList();
        return;
      }

      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
            String.format(
                "'sublist' is supported only on LIST type input. Given input type : %s",
                in.getMinorType().toString()));
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader =
          (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      final int size = listReader.size();

      final int startPosition =
          com.dremio.exec.expr.fn.impl.SubListFunction.resolveOffset(start.value, size);
      if (size == 0 || length.value <= 0 || startPosition <= 0 || startPosition > size) {
        // return if
        // 1. the input list is empty
        // 2. 0 - elements are requested
        // 3. invalid offset (offset starts beyond the length of the list)
        out.rootAsList();
        return;
      }
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      int currentPosition = 1;
      listWriter.startList();
      while (listReader.next() && currentPosition < startPosition + length.value) {
        if (currentPosition >= startPosition) {
          com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyListElements(listReader, listWriter);
        }
        currentPosition++;
      }
      listWriter.endList();
    }
  }

  public static int resolveOffset(int offset, int length) {
    if (offset < 0) {
      return length + offset + 1;
    } else if (offset == 0) {
      return 1; // Consider offset 0 as 1 (Same as substr behavior)
    }
    return offset;
  }

  public static final class SubListOutputDerivation implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 3);
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);

      return new CompleteType(ArrowType.List.INSTANCE, type.getChildren().get(0));
    }
  }
}
