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

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import javax.inject.Inject;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;

@FunctionTemplate(
    name = "convert_toBASE64",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class Base64ConvertTo implements SimpleFunction {

  @Param NullableVarCharHolder in;
  @Output VarBinaryHolder out;
  @Inject ArrowBuf buffer;

  @Override
  public void setup() {}

  @Override
  public void eval() {
    final String inputStr =
        com.dremio.exec.expr.fn.impl.StringFunctionHelpers.getStringFromNullableVarCharHolder(in);
    final byte[] outBytea = javax.xml.bind.DatatypeConverter.parseBase64Binary(inputStr);
    buffer = buffer.reallocIfNeeded(outBytea.length);
    out.buffer = buffer;
    out.buffer.setBytes(0, outBytea);
    out.start = 0;
    out.end = outBytea.length;
  }
}
