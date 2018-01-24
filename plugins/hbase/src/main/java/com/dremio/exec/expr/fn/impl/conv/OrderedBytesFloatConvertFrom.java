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

import javax.inject.Inject;

import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.VarBinaryHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.fn.FunctionErrorContext;

@FunctionTemplate(names = {"convert_fromFLOAT_OB", "convert_fromFLOAT_OBD"},
    scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
public class OrderedBytesFloatConvertFrom implements SimpleFunction {

  @Param VarBinaryHolder in;
  @Output Float4Holder out;
  @Workspace byte[] bytes;
  @Workspace org.apache.hadoop.hbase.util.PositionedByteRange br;
  @Inject FunctionErrorContext errorContext;

  @Override
  public void setup() {
    bytes = new byte[5];
    br = new org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange();
  }

  @Override
  public void eval() {
    com.dremio.exec.util.ByteBufUtil.checkBufferLength(errorContext, in.buffer, in.start, in.end, 5);
    in.buffer.getBytes(in.start, bytes, 0, 5);
    br.set(bytes);
    out.value = org.apache.hadoop.hbase.util.OrderedBytes.decodeFloat32(br);
  }
}
