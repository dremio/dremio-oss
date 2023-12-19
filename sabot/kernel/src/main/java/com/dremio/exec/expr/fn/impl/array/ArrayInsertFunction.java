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

import javax.inject.Inject;

import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

public class ArrayInsertFunction {

  @FunctionTemplate(name = "array_insert", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL,  derivation = InserOutputDerivation.class)
  public static class ArrayInsert implements SimpleFunction {
    @Param private FieldReader in;
    @Param private NullableIntHolder index;
    @Param private FieldReader value;
    @Output
    private BaseWriter.ComplexWriter out;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private Object inputValue;


    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      inputValue = value.readObject();
      if (!in.isSet() || in.readObject() == null || index.isSet == 0) {
        return;
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      final int size = listReader.size();
      final int position = com.dremio.exec.expr.fn.impl.array.ArrayInsertFunction.resolveIndex(index.value, size);

      int currentIndex  = 0;
      listWriter.startList();
      if(position >= size) {
        /*
          * If the position is greater than the size of the list, we need to write nulls until we reach the position
         */
        for(currentIndex = 0; currentIndex <= position; currentIndex++) {
          if(listReader.next()) {
            com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyListElements(listReader, listWriter);
          } else if (currentIndex == position) {
            com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyToList(value, listWriter);
          } else {
            listWriter.writeNull();
          }
        }
      } else {
        /*
          * If the position is less than the size of the list, we need to write the value at the position and then
          * write the remaining elements of the list
         */
        while (listReader.next()) {
          if (currentIndex == position) {
            com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyToList(value, listWriter);
          }
          com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyListElements(listReader, listWriter);
          currentIndex++;
        }
      }
      listWriter.endList();
    }
  }

  public static int resolveIndex(int index, int length) {
    if (index < 0) {
      return length + index;
    }
    return index;
  }

  public static final class InserOutputDerivation implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      CompleteType type = args.get(0).getCompleteType();
      Preconditions.checkArgument(type.getChildren().size() == 1);
      return args.get(0).getCompleteType();
    }
  }
}
