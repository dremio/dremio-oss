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

package com.dremio.exec.expr.fn.impl.conv;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation.Dummy;

public class JsonConvertFrom {

  private JsonConvertFrom() {}

  @FunctionTemplate(name = "convert_fromJSON", isDeterministic = false, derivation = Dummy.class)
  public static class ConvertFromJson implements SimpleFunction {

    @Param
    NullableVarBinaryHolder in;
    @Inject
    ArrowBuf buffer;
    @Workspace
    com.dremio.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;
    @Inject
    FunctionErrorContext errCtx;

    public void setup() {
      throw errCtx.error()
        .message("Operation not supported")
        .build();
    }

    public void eval() {
      throw errCtx.error()
        .message("Operation not supported")
        .build();
    }
  }

  @FunctionTemplate(name = "convert_fromJSON", isDeterministic = false, derivation = Dummy.class)
  public static class ConvertFromJsonVarchar implements SimpleFunction {

    @Param
    NullableVarCharHolder in;
    @Inject
    ArrowBuf buffer;
    @Workspace
    com.dremio.exec.vector.complex.fn.JsonReader jsonReader;

    @Output
    ComplexWriter writer;
    @Inject
    FunctionErrorContext errCtx;

    public void setup() {
      throw errCtx.error()
        .message("Operation not supported")
        .build();
    }

    public void eval() {
      throw errCtx.error()
        .message("Operation not supported")
        .build();
    }
  }

}
