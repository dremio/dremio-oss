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
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.fn.FunctionErrorContext;

// Function called on convert_from(<string>, 'UTF8', '?') -- with the third argument being the replacement character (empty string OK)
@SuppressWarnings("unused")
@FunctionTemplate(name = "convert_replaceUTF8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class UTF8ConvertReplace implements SimpleFunction {

  @Param  VarBinaryHolder in;
  @Param  VarCharHolder replace;
  @Inject ArrowBuf buffer;
  @Inject FunctionErrorContext errorContext;
  @Output VarCharHolder out;

  @Override
  public void setup() { }

  @Override
  public void eval() {
    if (replace.end > replace.start + 1) {
      throw errorContext.error()
        .message("In convert_from(<input>, 'UTF8', <replace string>), replace string must be 0 or 1 characters long")
        .build();

    }
    buffer = buffer.reallocIfNeeded(in.end - in.start);
    out.start = 0;
    if (replace.end == replace.start) {
      out.end = com.dremio.exec.expr.fn.impl.StringFunctionUtil.copyUtf8(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer), in.start, in
        .end, buffer);
    } else {
      out.end = com.dremio.exec.expr.fn.impl.StringFunctionUtil.copyReplaceUtf8(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer), in
        .start, in.end, io.netty.buffer.NettyArrowBuf.unwrapBuffer(buffer), replace.buffer.getByte(replace.start));
    }
    out.buffer = buffer;
  }
}
