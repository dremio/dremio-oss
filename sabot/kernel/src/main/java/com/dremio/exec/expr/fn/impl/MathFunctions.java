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

import java.text.DecimalFormat;

import javax.inject.Inject;

import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;

public class MathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

  private MathFunctions(){}

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Negative implements SimpleFunction{

    @Param BigIntHolder input;
    @Output BigIntHolder out;

    public void setup(){}

    public void eval(){
      out.value = -input.value;
      return;
    }

  }

  @FunctionTemplate(name = "power", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Power implements SimpleFunction{

    @Param Float8Holder a;
    @Param Float8Holder b;
    @Output  Float8Holder out;

    public void setup(){}

    public void eval(){
      out.value = java.lang.Math.pow(a.value, b.value);
    }

  }

  @FunctionTemplate(name = "random", isDeterministic = false)
  public static class Random implements SimpleFunction{
    @Output NullableFloat8Holder out;

    public void setup(){}

    public void eval(){
      out.isSet = 1;
      out.value = java.lang.Math.random();
    }

  }

  @FunctionTemplate(name = "to_number", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ToNumber implements SimpleFunction {
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Workspace java.text.DecimalFormat inputFormat;
    @Workspace int decimalDigits;
    @Output Float8Holder out;
    @Inject FunctionErrorContext errCtx;

    public void setup() {
      byte[] buf = new byte[right.end - right.start];
      right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
      inputFormat = new DecimalFormat(new String(buf));
      decimalDigits = inputFormat.getMaximumFractionDigits();
    }

    public void eval() {
      byte[] buf1 = new byte[left.end - left.start];
      left.buffer.getBytes(left.start, buf1, 0, left.end - left.start);
      String input = new String(buf1);
      try {
        out.value = inputFormat.parse(input).doubleValue();
      }  catch(java.text.ParseException e) {
        throw errCtx.error()
          .message("Cannot parse input: " + input + " with pattern : " + inputFormat.toPattern())
          .build();
      }

      // Round the value
      java.math.BigDecimal roundedValue = java.math.BigDecimal.valueOf(out.value);
      out.value = (roundedValue.setScale(decimalDigits, java.math.BigDecimal.ROUND_HALF_UP)).doubleValue();
    }
  }

  @FunctionTemplate(name = "pi", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Pi implements SimpleFunction {

    @Output Float8Holder out;

    public void setup() {
    }

    public void eval() {
        out.value = java.lang.Math.PI;
    }
  }

}
