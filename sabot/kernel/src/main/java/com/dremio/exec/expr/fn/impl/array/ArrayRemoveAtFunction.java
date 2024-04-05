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
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.holders.IntHolder;

public class ArrayRemoveAtFunction {
  @FunctionTemplate(
      names = "array_remove_at",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL,
      derivation = ListWithoutRemovedElements.class)
  public static class ArrayRemoveAt implements SimpleFunction {

    @Param private FieldReader in;
    @Param private IntHolder index;
    @Output private BaseWriter.ComplexWriter out;
    @Workspace private IntHolder currentIndex;
    @Inject private FunctionErrorContext errCtx;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      currentIndex.value = 0;
      if (!in.isSet() || in.readObject() == null) {
        return;
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader =
          (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      if (index.value < 0) {
        index.value = listReader.size() + index.value;
      }
      org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter listWriter = out.rootAsList();
      if (listReader.size() <= index.value) {
        org.apache.arrow.vector.complex.impl.ComplexCopier.copy(
            listReader, (org.apache.arrow.vector.complex.writer.FieldWriter) listWriter);
      } else {
        listWriter.startList();
        while (listReader.next()) {
          if (currentIndex.value != index.value) {
            com.dremio.exec.expr.fn.impl.array.ArrayHelper.copyListElements(listReader, listWriter);
          }
          currentIndex.value++;
        }
        listWriter.endList();
      }
    }
  }

  public static class ListWithoutRemovedElements implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 2);
      return args.get(0).getCompleteType();
    }
  }
}
