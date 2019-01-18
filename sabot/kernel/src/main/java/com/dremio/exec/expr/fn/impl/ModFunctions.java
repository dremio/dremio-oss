/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.IntHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;

public class ModFunctions {

  /**
   * Mod Functions should take in two inputs, input1 and input2.  The return type should be that of input2.
   * Also, mod(float, float) does not make sense, so inputs are restricted to int/bigint/smallint/tinyint/etc.
   */

  @FunctionTemplate(name = "mod", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ModInt implements SimpleFunction {

    @Param
    BigIntHolder input1;
    @Param IntHolder input2;
    @Output IntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = (int) (input2.value == 0 ? input1.value : (input1.value  %  input2.value));
    }
  }

  @FunctionTemplate(name = "mod", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ModBigInt implements SimpleFunction {

    @Param BigIntHolder input1;
    @Param BigIntHolder input2;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {
      out.value = (long) (input2.value == 0 ? input1.value : (input1.value  %  input2.value));
    }
  }

}
