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

import org.apache.arrow.vector.holders.IntHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;


public class RoundFunctions {

  /*
   * Following are round functions with no parameter. Per the SQL standard we simply return the same output
   * type as the input type for exact inputs (int, bigint etc) and inexact types (float, double).
   *
   * TODO: Need to incorporate round function which accepts two parameters here.
   */
  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class RoundInt implements SimpleFunction {

    @Param  IntHolder in;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundBigInt implements SimpleFunction {

    @Param BigIntHolder in;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = in.value;
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundFloat4 implements SimpleFunction {

    @Param Float4Holder in;
    @Output Float4Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = java.math.BigDecimal.valueOf(in.value);
      out.value = input.setScale(0, java.math.RoundingMode.HALF_UP).floatValue();
    }
  }

  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RoundFloat8 implements SimpleFunction {

    @Param Float8Holder in;
    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
      java.math.BigDecimal input = java.math.BigDecimal.valueOf(in.value);
      out.value = input.setScale(0, java.math.RoundingMode.HALF_UP).doubleValue();
    }
  }
}
