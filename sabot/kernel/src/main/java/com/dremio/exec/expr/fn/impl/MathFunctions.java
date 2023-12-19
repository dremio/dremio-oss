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

import java.text.DecimalFormat;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.ObjectHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;

public class MathFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MathFunctions.class);

  private MathFunctions(){}

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Negative implements SimpleFunction{

    @Param BigIntHolder input;
    @Output BigIntHolder out;

    @Override
    public void setup(){}

    @Override
    public void eval(){
      out.value = -input.value;
      return;
    }

  }

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class NegativeFloat8 implements SimpleFunction{

    @Param Float8Holder input;
    @Output Float8Holder out;

    @Override
    public void setup(){}

    @Override
    public void eval(){
      out.value = -input.value;
      return;
    }

  }

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class NegativeFloat4 implements SimpleFunction{

    @Param Float4Holder input;
    @Output Float4Holder out;

    @Override
    public void setup(){}

    @Override
    public void eval(){
      out.value = -input.value;
      return;
    }

  }

  @FunctionTemplate(names = {"negative", "u-", "-"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalNegativeScale.class)
  public static class NegativeDecimal implements SimpleFunction{

    @Param
    DecimalHolder in;
    @Output
    DecimalHolder out;
    @Inject
    FunctionErrorContext errorContext;

    @Inject
    ArrowBuf buffer;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      out.precision = in.precision;
      out.scale = in.scale;

      java.math.BigDecimal result = input.negate();
      result = result.setScale(in.scale);

      if (com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result, in.precision)) {
        result = new java.math.BigDecimal("0.0");
      }

      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
          .build();
      }

      out.buffer = buffer;
    }

  }

  @FunctionTemplate(name = "power", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Power implements SimpleFunction{

    @Param Float8Holder a;
    @Param Float8Holder b;
    @Output  Float8Holder out;

    @Override
    public void setup(){}

    @Override
    public void eval(){
      out.value = java.lang.Math.pow(a.value, b.value);
    }

  }

  @FunctionTemplate(names = {"random", "rand"}, isDeterministic = false)
  public static class Random implements SimpleFunction {
    @Output
    NullableFloat8Holder out;
    @Workspace
    java.util.Random random;

    @Override
    public void setup() {
      random = new java.util.Random();
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = random.nextDouble();
    }
  }

  @FunctionTemplate(names = "sample", isDeterministic = false)
  public static class Sample implements SimpleFunction {
    @Param Float8Holder rate;
    @Output private NullableBitHolder out;
    @Workspace private ObjectHolder random;
    @Workspace private IntHolder samplingRate;

    @Override
    public void setup() {
      random = new ObjectHolder();
      random.obj = new java.util.SplittableRandom();
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = ((java.util.SplittableRandom) random.obj).nextDouble() * 100  < rate.value ? 1 : 0;
    }
  }

  @FunctionTemplate(names = {"random", "rand"}, isDeterministic = false, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class RandomWithSeed implements SimpleFunction {

    @Param
    NullableIntHolder seedHolder;
    @Output
    NullableFloat8Holder out;
    @Workspace
    java.util.Random random;

    @Override
    public void setup() {
      int seed = 0;
      if (seedHolder.isSet == 1) {
        seed = seedHolder.value;
      }
      random = new java.util.Random(seed);
    }

    @Override
    public void eval() {
      out.isSet = 1;
      out.value = random.nextDouble();
    }
  }

  @FunctionTemplate(name = "bitwise_not", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class IntBitwiseNot implements SimpleFunction {

    @Param IntHolder in;
    @Output IntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.value = ~in.value;
    }
  }

  @FunctionTemplate(name = "bitwise_not", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BigIntBitwiseNot implements SimpleFunction {

    @Param BigIntHolder in;
    @Output BigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.value = ~in.value;
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

    @Override
    public void setup() {
      byte[] buf = new byte[right.end - right.start];
      right.buffer.getBytes(right.start, buf, 0, right.end - right.start);
      inputFormat = new DecimalFormat(new String(buf));
      decimalDigits = inputFormat.getMaximumFractionDigits();
    }

    @Override
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

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
        out.value = java.lang.Math.PI;
    }
  }

  @FunctionTemplate(name = "factorial", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Factorial implements SimpleFunction {

    @Param BigIntHolder in;
    @Output BigIntHolder out;
    @Inject FunctionErrorContext errCtx;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      final Long[] factorialLookupTable = {1L, 1L, 2L, 6L, 24L, 120L, 720L, 5040L, 40320L, 362880L,
        3628800L, 39916800L, 479001600L, 6227020800L, 87178291200L, 1307674368000L, 20922789888000L,
        355687428096000L, 6402373705728000L, 121645100408832000L, 2432902008176640000L};
      if (in.value > 20) {
        throw errCtx.error()
          .message("Numbers greater than 20 cause overflow!")
          .build();
      } else if (in.value < 0) {
        throw errCtx.error()
          .message("Factorial of negative number not exist!")
          .build();
      } else {
        out.value = factorialLookupTable[Long.valueOf(in.value).intValue()];
      }
    }
  }
}
