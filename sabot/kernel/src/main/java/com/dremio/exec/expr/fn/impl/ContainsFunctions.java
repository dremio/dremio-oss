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

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import javax.inject.Inject;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

public class ContainsFunctions {

  @FunctionTemplate(name = "contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Contains implements SimpleFunction {

    @Param FieldReader field;
    @Param VarCharHolder query;
    @Output NullableBitHolder out;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (true) {
        throw errCtx.error().message("Contains function is not supported in this context").build();
      }
    }
  }
}
