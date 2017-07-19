/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

@FunctionTemplate(name = "convert_fromBOOLEAN_BYTE", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class BooleanByteConvertFrom implements SimpleFunction {

  @Param VarBinaryHolder in;
  @Output BitHolder out;

  @Override
  public void setup() { }

  @Override
  public void eval() {
    com.dremio.exec.util.ByteBufUtil.checkBufferLength(in.buffer, in.start, in.end, 1);

    in.buffer.readerIndex(in.start);
    out.value = in.buffer.readByte()==0 ? 0 : 1;
  }
}
