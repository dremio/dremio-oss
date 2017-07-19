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

import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.IntHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;

/**
 * Function templates for Bit/BOOLEAN functions other than comparison
 * functions.  (Bit/BOOLEAN comparison functions are generated in
 * ComparisonFunctions.java template.)
 *
 */
public class BitFunctions {


  @FunctionTemplate(name = "partitionBitCounter", nulls = NullHandling.NULL_IF_NULL)
  public static class BitCounter implements SimpleFunction {
    @Param BitHolder input;
    @Workspace int partitionNumber;
    @Output IntHolder out;

    public void setup() {
      partitionNumber = 0;
    }

    public void eval() {
      partitionNumber += input.value;
      out.value = partitionNumber;
    }

  }
  @FunctionTemplate(names = {"booleanOr", "or", "||", "orNoShortCircuit"},
                    nulls = NullHandling.NULL_IF_NULL)
  public static class BitOr implements SimpleFunction {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value | right.value;
    }
  }

  @FunctionTemplate(names = {"booleanAnd", "and", "&&"},
                    nulls = NullHandling.NULL_IF_NULL)
  public static class BitAnd implements SimpleFunction {

    @Param BitHolder left;
    @Param BitHolder right;
    @Output BitHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value & right.value;
    }
  }


  @FunctionTemplate(names = {"xor", "^"},
                    scope = FunctionScope.SIMPLE,
                    nulls = NullHandling.NULL_IF_NULL)
  public static class IntXor implements SimpleFunction {

    @Param IntHolder left;
    @Param IntHolder right;
    @Output IntHolder out;

    public void setup() {}

    public void eval() {
      out.value = left.value ^ right.value;
    }
  }

}
