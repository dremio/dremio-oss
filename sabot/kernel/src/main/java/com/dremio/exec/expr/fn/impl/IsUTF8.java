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

import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

// Check if the input varchar is a UTF-8 string (i.e., all its characters are UTF-8 characters)
@SuppressWarnings("unused")
@FunctionTemplate(name = "is_UTF8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class IsUTF8 implements SimpleFunction {
  @Param  VarCharHolder in;
  @Output BitHolder out;

  @Override
  public void setup() { }

  @Override
  public void eval() {
    out.value = (com.dremio.exec.expr.fn.impl.GuavaUtf8.isUtf8(io.netty.buffer.NettyArrowBuf.unwrapBuffer(in.buffer), in.start, in.end) ? 1
      : 0);
  }
}
