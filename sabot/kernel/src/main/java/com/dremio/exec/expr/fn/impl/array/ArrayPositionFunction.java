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

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;

public class ArrayPositionFunction {

  @FunctionTemplate(
      name = "array_position",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = FunctionTemplate.NullHandling.INTERNAL)
  public static class ArrayPosition implements SimpleFunction {

    @Param(constant = true)
    private FieldReader value;

    @Param private FieldReader in;
    @Output private NullableIntHolder out;
    @Inject private FunctionErrorContext errCtx;
    @Workspace private Object inputValue;
    @Workspace private IntHolder index;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      inputValue = value.readObject();
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader =
          (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      index.value = 0;
      while (listReader.next()) {
        if (java.util.Objects.equals(listReader.reader().readObject(), inputValue)) {
          out.value = index.value;
          out.isSet = 1;
          return;
        }
        index.value++;
      }
      out.isSet = 0;
    }
  }
}
