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
package com.dremio.exec.expr.fn.impl;

import javax.inject.Inject;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

import io.netty.buffer.ArrowBuf;

public class SimpleCastFunctions {
  public static final byte[] TRUE = {'t','r','u','e'};
  public static final byte[] FALSE = {'f','a','l','s','e'};


  @FunctionTemplate(names = {"castBIT", "castBOOLEAN"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharBoolean implements SimpleFunction {

    @Param VarCharHolder in;
    @Output BitHolder out;

    public void setup() {

    }

    public void eval() {
      byte[] buf = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
      String input = new String(buf, com.google.common.base.Charsets.UTF_8).toLowerCase();
      if ("true".equals(input) || "1".equals(input)) {
        out.value = 1;
      } else if ("false".equals(input) || "0".equals(input)) {
        out.value = 0;
      } else {
        throw new IllegalArgumentException("Invalid value for boolean: " + input);
      }
    }
  }

  @FunctionTemplate(name = "castVARCHAR", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastBooleanVarChar implements SimpleFunction {

    @Param BitHolder in;
    @Param BigIntHolder len;
    @Output VarCharHolder out;
    @Inject ArrowBuf buffer;

    public void setup() {}

    public void eval() {
      byte[] outB = in.value == 1 ? com.dremio.exec.expr.fn.impl.SimpleCastFunctions.TRUE : com.dremio.exec.expr.fn.impl.SimpleCastFunctions.FALSE;
      buffer.setBytes(0, outB);
      out.buffer = buffer;
      out.start = 0;
      out.end = Math.min((int)len.value, outB.length); // truncate if target type has length smaller than that of input's string
    }
  }

}
