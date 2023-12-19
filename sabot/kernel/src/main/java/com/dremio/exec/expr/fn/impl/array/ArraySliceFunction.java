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

import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

public class ArraySliceFunction {
  @FunctionTemplate(name = "array_slice", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = SliceOutputDerivation.class)
  public static class ArraySliceFromTo implements SimpleFunction {
    @Param
    private FieldReader in;
    @Param
    private NullableIntHolder from;
    @Param
    private NullableIntHolder to;
    @Output
    private BaseWriter.ComplexWriter out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null || from.isSet == 0 || to.isSet == 0) {
        return;
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      final int size = listReader.size();

      final int fromPosition = com.dremio.exec.expr.fn.impl.array.ArraySliceFunction.resolvePosition(from.value, size);
      final int toPosition = com.dremio.exec.expr.fn.impl.array.ArraySliceFunction.resolvePosition(to.value, size);
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      com.dremio.exec.expr.fn.impl.array.ArraySliceFunction.sliceArray(fromPosition, toPosition, listReader, listWriter);
    }
  }

  @FunctionTemplate(name = "array_slice", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = SliceOutputDerivation.class)
  public static class ArraySliceFrom implements SimpleFunction {
    @Param
    private FieldReader in;
    @Param
    private NullableIntHolder from;
    @Output
    private BaseWriter.ComplexWriter out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null || from.isSet == 0) {
        return;
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      final int size = listReader.size();

      final int fromPosition = com.dremio.exec.expr.fn.impl.array.ArraySliceFunction.resolvePosition(from.value, size);
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      com.dremio.exec.expr.fn.impl.array.ArraySliceFunction.sliceArray(fromPosition, size, listReader, listWriter);
    }
  }

  public static void sliceArray(int fromPosition, int toPosition,
    UnionListReader listReader, ListWriter listWriter) {
    final int size = listReader.size();
    listWriter.startList();
    if (size == 0 || fromPosition < 0 || toPosition < 0 || fromPosition > toPosition
      || toPosition > size) {
      // return empty list for invalid input
      listWriter.endList();
      return;
    }
    int arrayFirstPosition = ArraySliceFunction.getFirstPosition(listReader);
    for (int i = fromPosition; i < toPosition; i++) {
      listReader.reader().setPosition(arrayFirstPosition + i);
      ArrayHelper.copyListElements(listReader, listWriter);
    }
    listWriter.endList();
  }

  public static int getFirstPosition(org.apache.arrow.vector.complex.impl.UnionListReader listReader) {
    /*
     * Need call next() to move the reader to the first element for array column with several rows.
     * In this case values will be merged to one vector and need to get the first position for each row.
     * This allows function always start from needed position.
     */
    listReader.next();
    return listReader.reader().getPosition();
  }

  public static int resolvePosition(int position, int length) {
    if (position < 0) {
      return length + position;
    }
    return position;
  }

  public static final class SliceOutputDerivation implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);
      return args.get(0).getCompleteType();
    }
  }
}
