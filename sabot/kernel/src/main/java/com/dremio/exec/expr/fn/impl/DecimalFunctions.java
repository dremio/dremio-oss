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

import java.math.BigDecimal;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.BigIntHolder;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.Float4Holder;
import org.apache.arrow.vector.holders.Float8Holder;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.dremio.exec.expr.AggrFunction;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.FunctionTemplate.FunctionScope;
import com.dremio.exec.expr.annotations.FunctionTemplate.NullHandling;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.FunctionGenerationHelper;
import com.dremio.exec.expr.fn.OutputDerivation;


public class DecimalFunctions {

  public static final String DECIMAL_CAST_NULL_ON_OVERFLOW = "castDECIMALNullOnOverflow";

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castVARCHAR"}, scope = FunctionScope.SIMPLE, nulls= NullHandling.NULL_IF_NULL)
  public static class CastDecimalVarChar implements SimpleFunction {

    @Param
    DecimalHolder in;
    @Param
    BigIntHolder len;
    @Output
    VarCharHolder out;
    @Inject
    ArrowBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      String istr = bd.toString();
      out.start = 0;
      out.end = Math.min((int)len.value, istr.length()); // truncate if target type has length smaller than that of input's string
      buffer = buffer.reallocIfNeeded(out.end);
      out.buffer = buffer;
      out.buffer.setBytes(0, istr.substring(0,out.end).getBytes());
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castFLOAT8"}, scope = FunctionScope.SIMPLE, nulls= NullHandling.NULL_IF_NULL)
  public static class CastDecimalFloat8 implements SimpleFunction {

    @Param
    DecimalHolder in;
    @Output
    Float8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      out.value = bd.doubleValue();
    }
  }

  public static void main(String[] args) {
    BigDecimal bd = new BigDecimal("99.0000");
    System.out.println(bd.toString());
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.NULL_IF_NULL)
  public static class CastVarCharDecimal implements SimpleFunction {

    @Param
    VarCharHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      String s = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
      java.math.BigDecimal bd = new java.math.BigDecimal(s).setScale((int) scale.value, java.math.RoundingMode.HALF_UP);

      // Decimal value will be 0 if there is precision overflow.
      // This is similar to DecimalFunctions:CastDecimalDecimal
      if (com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(bd, (int) precision.value)) {
        bd = new java.math.BigDecimal("0.0");
      }

      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(bd, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
            .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {DECIMAL_CAST_NULL_ON_OVERFLOW}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.INTERNAL)
  public static class CastVarcharDecimalNullOnOverflow implements SimpleFunction {

    @Param
    NullableVarCharHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    NullableDecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;
    @Workspace
    IntHolder expectedSignificantDigits;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
      expectedSignificantDigits.value = (int)(precision.value - scale.value);
    }

    @Override
    public void eval() {
      out.isSet = in.isSet;
      if (in.isSet == 1) {
        String s = com.dremio.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
        java.math.BigDecimal originalValue = new java.math.BigDecimal(s);
        java.math.BigDecimal convertedValue = originalValue.setScale((int) scale.value, java.math.RoundingMode
          .HALF_UP);
        int significantDigitsConverted = convertedValue.precision() - convertedValue.scale();

        if (significantDigitsConverted > expectedSignificantDigits.value) {
          out.isSet = 0;
        } else {
          out.isSet = 1;
          try {
            org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(convertedValue, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
          } catch (RuntimeException e) {
            throw errorContext.error(e)
              .build();
          }
          out.buffer = buffer;
          out.precision = (int) precision.value;
          out.scale = (int) scale.value;
        }
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {DECIMAL_CAST_NULL_ON_OVERFLOW}, derivation = OutputDerivation.DecimalCast
          .class, nulls= NullHandling.INTERNAL)
  public static class CastDecimalDecimalNullOnOverflow implements SimpleFunction {

    @Param
    NullableDecimalHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    NullableDecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      out.isSet = in.isSet;
      if (in.isSet == 1) {
        long index = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
        java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        java.math.BigDecimal result = input.setScale((int) scale.value, java.math.RoundingMode.HALF_UP);
        boolean overflow = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result,
          (int) precision.value);
        if (overflow) {
          out.isSet = 0;
        } else {
          out.isSet = 1;
          try {
            org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
          } catch (RuntimeException e) {
            throw errorContext.error(e)
              .build();
          }
          out.buffer = buffer;
          out.precision = (int) precision.value;
          out.scale = (int) scale.value;
        }
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.NULL_IF_NULL)
  public static class CastDecimalDecimal implements SimpleFunction {

    @Param
    DecimalHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.roundWithPositiveScale(input,
              (int) scale.value, java.math.RoundingMode.HALF_UP);

      if (com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result, (int) precision.value)) {
        result = new java.math.BigDecimal("0.0");
      }

      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
                .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.NULL_IF_NULL)
  public static class CastIntDecimal implements SimpleFunction {

    @Param
    IntHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      java.math.BigDecimal bd = java.math.BigDecimal.valueOf(in.value).setScale((int) scale.value, java.math.RoundingMode.HALF_UP);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(bd, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
            .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.NULL_IF_NULL)
  public static class CastBigIntDecimal implements SimpleFunction {

    @Param
    BigIntHolder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      java.math.BigDecimal bd = java.math.BigDecimal.valueOf(in.value).setScale((int) scale.value, java.math.RoundingMode.HALF_UP);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(bd, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
            .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class)
  public static class CastFloat4Decimal implements SimpleFunction {

    @Param
    Float4Holder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    NullableDecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      out.isSet = 1;
      java.math.BigDecimal bd = java.math.BigDecimal.valueOf(in.value).setScale((int) scale.value, java.math.RoundingMode.HALF_UP);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(bd, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
            .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castDECIMAL"}, derivation = OutputDerivation.DecimalCast.class, nulls= NullHandling.NULL_IF_NULL)
  public static class CastFloat8Decimal implements SimpleFunction {

    @Param
    Float8Holder in;
    @Param(constant = true)
    BigIntHolder precision;
    @Param(constant = true)
    BigIntHolder scale;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      java.math.BigDecimal bd = java.math.BigDecimal.valueOf(in.value).setScale((int) scale.value, java.math.RoundingMode.HALF_UP);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(bd, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
            .build();
      }
      out.buffer = buffer;
      out.precision = (int) precision.value;
      out.scale = (int) scale.value;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "sum", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalSum implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableFloat8Holder sum;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    public void setup() {
      sum = new NullableFloat8Holder();
      sum.isSet = 1;
      sum.value = 0;
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      nonNullCount.value = 0;
    }
    public void add() {
      if (in.isSet != 0) {
        in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
        java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        sum.value += bd.doubleValue();
        nonNullCount.value++;
      }
    }
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.value = sum.value;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }
    public void reset() {
      sum.value = 0;
      nonNullCount.value = 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "$sum0", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalSumZero implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableFloat8Holder sum;
    @Output NullableFloat8Holder out;

    public void setup() {
      sum = new NullableFloat8Holder();
      sum.isSet = 1;
      sum.value = 0;
    }
    public void add() {
      if (in.isSet == 1) {
        in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
        java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale,org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        sum.value += bd.doubleValue();
      }
    }
    public void output() {
      out.isSet = 1;
      out.value = sum.value;
    }
    public void reset() {
      sum.value = 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "min", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalMin implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableFloat8Holder minVal;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    public void setup() {
      minVal = new NullableFloat8Holder();
      minVal.isSet = 1;
      minVal.value = Double.MAX_VALUE;
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      nonNullCount.value = 0;
    }
    public void add() {
      if (in.isSet != 0) {
        nonNullCount.value = 1;
        in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
        java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        double val = bd.doubleValue();
        if (val < minVal.value) {
          minVal.value = val;
        }
      }
    }
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.value = minVal.value;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }
    public void reset() {
      minVal.value = 0;
      nonNullCount.value = 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "max", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalMax implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableFloat8Holder maxVal;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableFloat8Holder out;

    public void setup() {
      maxVal = new NullableFloat8Holder();
      maxVal.isSet = 1;
      maxVal.value = -Double.MAX_VALUE;
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      nonNullCount.value = 0;
    }
    public void add() {
      if (in.isSet != 0) {
        nonNullCount.value = 1;
        in.start = (in.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
        java.math.BigDecimal bd = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), in.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        double val = bd.doubleValue();
        if (val > maxVal.value) {
          maxVal.value = val;
        }
      }
    }
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.value = maxVal.value;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }
    public void reset() {
      maxVal.value = 0;
      nonNullCount.value = 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "sum_v2", derivation = OutputDerivation.DecimalAggSum.class,
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalSumV2 implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableDecimalHolder sum;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableDecimalHolder out;
    @Inject ArrowBuf buffer;

    public void setup() {
      sum = new NullableDecimalHolder();
      sum.isSet = 1;
      buffer = buffer.reallocIfNeeded(16);
      sum.buffer = buffer;
      java.math.BigDecimal zero = new java.math.BigDecimal(java.math.BigInteger.ZERO, 0);
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(zero, sum.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      sum.start = 0;
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      nonNullCount.value = 0;
    }

    public void add() {
      if (in.isSet == 1) {
        com.dremio.exec.util.DecimalUtils.addSignedDecimalInLittleEndianBytes(sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start), in.buffer,
          org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start));
        nonNullCount.value++;
      }
    }

    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.buffer = sum.buffer;
        out.start = sum.start;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }

    public void reset() {
      nonNullCount.value = 0;
      java.math.BigDecimal zero = new java.math.BigDecimal(java.math.BigInteger.ZERO, 0);
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(zero, sum.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
    }
  }

  /**
   * Sum0 returns 0 instead of nulls in case all aggregation values
   * are null.
   */
  @SuppressWarnings("unused")
  @FunctionTemplate(name = "$sum0_v2", derivation = OutputDerivation.DecimalAggSum.class,
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalSumZeroV2 implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableDecimalHolder sum;
    @Output NullableDecimalHolder out;
    @Inject ArrowBuf buffer;

    public void setup() {
      sum = new NullableDecimalHolder();
      sum.isSet = 1;
      buffer = buffer.reallocIfNeeded(16);
      sum.buffer = buffer;
      sum.start = 0;
      java.math.BigDecimal zero = new java.math.BigDecimal(java.math.BigInteger.ZERO, 0);
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(zero, sum.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
    }

    public void add() {
      if (in.isSet == 1) {
        com.dremio.exec.util.DecimalUtils.addSignedDecimalInLittleEndianBytes(sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start), in.buffer,
          org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start));
      }
    }

    public void output() {
      out.isSet = 1;
      out.buffer = sum.buffer;
      out.start = sum.start;
    }

    public void reset() {
      java.math.BigDecimal zero = new java.math.BigDecimal(java.math.BigInteger.ZERO, 0);
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(zero, sum.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "min_v2", derivation = OutputDerivation.DecimalAggMinMax.class,
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalMinV2 implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableDecimalHolder minVal;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableDecimalHolder out;
    @Inject ArrowBuf buffer;

    public void setup() {
      minVal = new NullableDecimalHolder();
      minVal.isSet = 1;
      minVal.start = 0;
      buffer = buffer.reallocIfNeeded(16);
      minVal.buffer = buffer;
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MAX_DECIMAL, minVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      nonNullCount.value = 0;
    }

    public void add() {
      if (in.isSet != 0) {
        nonNullCount.value = 1;
        int compare = com.dremio.exec.util.DecimalUtils
          .compareSignedDecimalInLittleEndianBytes(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), minVal.buffer, 0);
        if (compare < 0) {
          in.buffer.getBytes(in.start, minVal.buffer, 0 , org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        }
      }
    }
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.buffer = minVal.buffer;
        out.start = 0;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }

    public void reset() {
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MAX_DECIMAL, minVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      nonNullCount.value = 0;
    }

  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "max_v2", derivation = OutputDerivation.DecimalAggMinMax.class,
    scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class NullableDecimalMaxV2 implements AggrFunction {
    @Param NullableDecimalHolder in;
    @Workspace NullableDecimalHolder maxVal;
    @Workspace NullableBigIntHolder nonNullCount;
    @Output NullableDecimalHolder out;
    @Inject ArrowBuf buffer;

    public void setup() {
      maxVal = new NullableDecimalHolder();
      maxVal.isSet = 1;
      maxVal.start = 0;
      buffer = buffer.reallocIfNeeded(16);
      maxVal.buffer = buffer;
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MIN_DECIMAL, maxVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      nonNullCount = new NullableBigIntHolder();
      nonNullCount.isSet = 1;
      nonNullCount.value = 0;
    }
    public void add() {
      if (in.isSet != 0) {
        nonNullCount.value = 1;
        int compare = com.dremio.exec.util.DecimalUtils
          .compareSignedDecimalInLittleEndianBytes(in.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(in.start), maxVal.buffer, 0);
        if (compare > 0) {
          in.buffer.getBytes(in.start, maxVal.buffer, 0 , org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        }
      }
    }
    public void output() {
      if (nonNullCount.value > 0) {
        out.isSet = 1;
        out.buffer = maxVal.buffer;
        out.start = 0;
      } else {
        // All values were null. Result should be null too
        out.isSet = 0;
      }
    }
    public void reset() {
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MIN_DECIMAL, maxVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      nonNullCount.value = 0;
    }

  }

  /**
   * Decimal comparator where null appears last i.e. nulls are considered
   * larger than all values.
   */
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_HIGH,
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = NullHandling.INTERNAL)
  public static class CompareDecimalVsDecimalNullsHigh implements SimpleFunction {

    @Param NullableDecimalHolder left;
    @Param NullableDecimalHolder right;
    @Output
    NullableIntHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      outside:
      {

        if ( left.isSet == 0 ) {
          if ( right.isSet == 0 ) {
            out.value = 0;
            break outside;
          } else {
            out.value = 1;
            break outside;
          }
        } else if ( right.isSet == 0 ) {
          out.value = -1;
          break outside;
        }

        out.value = com.dremio.exec.util.DecimalUtils.compareSignedDecimalInLittleEndianBytes
          (left.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(left.start), right.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(right.start));
      } // outside
    }
  }

  /**
   * Decimal comparator where null appears first i.e. nulls are considered
   * smaller than all values.
   */
  @FunctionTemplate(name = FunctionGenerationHelper.COMPARE_TO_NULLS_LOW,
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = NullHandling.INTERNAL)
  public static class CompareDecimalVsDecimalNullsLow implements SimpleFunction {

    @Param NullableDecimalHolder left;
    @Param NullableDecimalHolder right;
    @Output
    NullableIntHolder out;

    public void setup() {}

    public void eval() {
      out.isSet = 1;
      outside:
      {

        if ( left.isSet == 0 ) {
          if ( right.isSet == 0 ) {
            out.value = 0;
            break outside;
          } else {
            out.value = -1;
            break outside;
          }
        } else if ( right.isSet == 0 ) {
          out.value = 1;
          break outside;
        }

        out.value = com.dremio.exec.util.DecimalUtils.compareSignedDecimalInLittleEndianBytes
          (left.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(left.start), right.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(right.start));

      } // outside
    }
  }

  public static BigDecimal checkOverflow(BigDecimal in) {
    return in.precision() > 38 ? new BigDecimal(0) : in;
  }

  public static boolean checkOverflow(BigDecimal in, int precision) {
    return in.precision() > precision;
  }

  public static BigDecimal addOrSubtract(boolean isSubtract, BigDecimal left, BigDecimal right,
                                   int outPrecision, int outScale) {

    if (isSubtract) {
      right = right.negate();
    }

    int higherScale = Math.max(left.scale(), right.scale()); // >= outScale
    BigDecimal leftScaled = left.setScale(higherScale, BigDecimal.ROUND_UNNECESSARY);
    BigDecimal rightScaled = right.setScale(higherScale, BigDecimal.ROUND_UNNECESSARY);
    BigDecimal result = leftScaled.add(rightScaled);

    if (higherScale > outScale) {
      result = result.setScale(outScale, BigDecimal.ROUND_HALF_UP);
    }
    return checkOverflow(result);
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, derivation = OutputDerivation.DecimalAdd.class, nulls = NullHandling.NULL_IF_NULL)
  public static class AddTwoDecimals implements SimpleFunction {

    @Param
    DecimalHolder in1;
    @Param
    DecimalHolder in2;
    @Output
    DecimalHolder out;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext errorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (in1.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in1.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), in1.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (in2.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(in2.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), in2.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      org.apache.arrow.vector.types.pojo.ArrowType.Decimal resultTypeForOperation = org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.getResultTypeForOperation(org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType.ADD,
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(in1.precision, in1.scale),
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(in2.precision, in2.scale));
      out.precision = resultTypeForOperation.getPrecision();
      out.scale = resultTypeForOperation.getScale();

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.addOrSubtract(false, left, right, out.precision, out.scale);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw errorContext.error(e)
          .build();
      }
      out.buffer = buffer;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "subtract", scope = FunctionScope.SIMPLE, derivation = OutputDerivation.DecimalSubtract.class, nulls = NullHandling.NULL_IF_NULL)
  public static class SubtractDecimals implements SimpleFunction {

    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      org.apache.arrow.vector.types.pojo.ArrowType.Decimal resultTypeForOperation = org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.getResultTypeForOperation(org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType.SUBTRACT,
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(leftHolder.precision, leftHolder.scale),
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(rightHolder.precision, rightHolder.scale));
      resultHolder.precision = resultTypeForOperation.getPrecision();
      resultHolder.scale = resultTypeForOperation.getScale();

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.addOrSubtract(true, left, right, resultHolder.precision, resultHolder.scale);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "multiply", derivation = OutputDerivation.DecimalMultiply.class, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class MultiplyDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      org.apache.arrow.vector.types.pojo.ArrowType.Decimal resultTypeForOperation = org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.getResultTypeForOperation(org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType.MULTIPLY,
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(leftHolder.precision, leftHolder.scale),
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(rightHolder.precision, rightHolder.scale));
      resultHolder.precision = resultTypeForOperation.getPrecision();
      resultHolder.scale = resultTypeForOperation.getScale();

      java.math.BigDecimal result = left.multiply(right).setScale(resultHolder.scale, java.math.BigDecimal.ROUND_HALF_UP);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "divide", derivation = OutputDerivation.DecimalDivide.class, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class DivideDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {

      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      org.apache.arrow.vector.types.pojo.ArrowType.Decimal resultTypeForOperation = org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.getResultTypeForOperation(org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType.DIVIDE,
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(leftHolder.precision, leftHolder.scale),
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(rightHolder.precision, rightHolder.scale));
      resultHolder.precision = resultTypeForOperation.getPrecision();
      resultHolder.scale = resultTypeForOperation.getScale();

      if (resultHolder.scale > leftHolder.scale - rightHolder.scale) {
        left = left.setScale(resultHolder.scale + rightHolder.scale, java.math.BigDecimal.ROUND_UNNECESSARY);
      }
      java.math.BigInteger leftUnscaled = left.unscaledValue();
      java.math.BigInteger rightUnscaled = right.unscaledValue();
      java.math.BigInteger[] quotientAndRemainder = leftUnscaled.divideAndRemainder(rightUnscaled);
      java.math.BigInteger resultUnscaled = quotientAndRemainder[0];
      if (quotientAndRemainder[1].abs().multiply(java.math.BigInteger.valueOf(2)).compareTo
        (rightUnscaled.abs()) >= 0) {
        resultUnscaled = resultUnscaled.add(java.math.BigInteger.valueOf((leftUnscaled.signum() ^
          rightUnscaled.signum()) + 1));
      }
      java.math.BigDecimal result = new java.math.BigDecimal(resultUnscaled, resultHolder.scale);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"modulo", "mod"}, derivation = OutputDerivation.DecimalMod.class, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ModuloFunction implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {

      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      org.apache.arrow.vector.types.pojo.ArrowType.Decimal resultTypeForOperation = org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.getResultTypeForOperation(org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType.MOD,
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(leftHolder.precision, leftHolder.scale),
        new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(rightHolder.precision, rightHolder.scale));
      resultHolder.precision = resultTypeForOperation.getPrecision();
      resultHolder.scale = resultTypeForOperation.getScale();


      if (leftHolder.scale < rightHolder.scale) {
        left = left.setScale(rightHolder.scale, java.math.BigDecimal.ROUND_UNNECESSARY);
      } else {
        right = right.setScale(leftHolder.scale, java.math.BigDecimal.ROUND_UNNECESSARY);
      }

      java.math.BigInteger leftUnscaled = left.unscaledValue();
      java.math.BigInteger rightUnscaled = right.unscaledValue();
      java.math.BigInteger remainder = leftUnscaled.remainder(rightUnscaled);
      java.math.BigDecimal result = new java.math.BigDecimal(remainder, resultHolder.scale);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"equal", "==", "="},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class EqualsDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) == 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(
    names = {"not_equal", "<>", "!="},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class NotEqualsDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) != 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(
    names = {"less_than", "<"},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class LessThanDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) < 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(
    names = {"less_than_or_equal_to", "<="},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class LessThanEqDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) <= 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(
    names = {"greater_than", ">"},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThanDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) > 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(
    names = {"greater_than_or_equal_to", ">="},
    scope = FunctionScope.SIMPLE,
    nulls = NullHandling.NULL_IF_NULL)
  public static class GreaterThanEqDecimals implements SimpleFunction {
    @Param
    DecimalHolder leftHolder;
    @Param
    DecimalHolder rightHolder;
    @Output
    BitHolder resultHolder;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      long index = (leftHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal left = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(leftHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), leftHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      index = (rightHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal right = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(rightHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), rightHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      resultHolder.value = (left.compareTo(right) >= 0) ? 1 : 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "abs", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalMax.class)
  public static class AbsDecimal implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {

      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = input.abs();
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      resultHolder.precision = inputHolder.precision;
      resultHolder.scale = inputHolder.scale;
    }
  }


  public static java.math.BigDecimal round(BigDecimal input, int scale, java.math.RoundingMode roundingMode) {
    if (scale < 0) {
      return com.dremio.exec.expr.fn.impl.DecimalFunctions.roundWithNegativeScale(input, scale, roundingMode);
    }
    return com.dremio.exec.expr.fn.impl.DecimalFunctions.roundWithPositiveScale(input, scale, roundingMode);
  }

  // scale is negative
  private static BigDecimal roundWithNegativeScale(BigDecimal input, int scale, java.math.RoundingMode roundingMode) {
    java.math.BigDecimal inputNoFractional = input.setScale(0, roundingMode);
    java.math.BigDecimal result = new java.math.BigDecimal(inputNoFractional.unscaledValue()
      .subtract(inputNoFractional.unscaledValue().remainder(java.math.BigInteger.TEN.pow(-scale))), scale);
    result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
    return result;
  }

  public static BigDecimal roundWithPositiveScale(BigDecimal input, int scale, java.math
          .RoundingMode roundingMode) {
    java.math.BigDecimal result = input.setScale(scale, roundingMode);
    result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
    return result;
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls =
    NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalSetScaleRound.class)
  public static class RoundDecimalWithScale implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Param(constant = true)
    IntHolder scale;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      java.math.BigDecimal result;
      if (scale.value > input.scale()) {
        result = input;
      } else {
        result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, scale.value, java.math.RoundingMode.HALF_UP);
        result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      }
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_SET_SCALE_ROUND.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale),
            new com.dremio.common.expression.ValueExpressions.IntExpression(scale.value)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "round", scope = FunctionScope.SIMPLE, nulls =
    NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalZeroScaleRound.class)
  public static class RoundDecimal implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, 0, java.math.RoundingMode.HALF_UP);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_ZERO_SCALE_ROUND.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"truncate", "trunc"}, scope = FunctionScope.SIMPLE, nulls =
    NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalSetScaleTruncate.class)
  public static class TruncateDecimalWithScale implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Param(constant = true)
    IntHolder scale;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      java.math.BigDecimal result;
      if (scale.value > input.scale()) {
        result = input;
      } else {
        result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, scale.value, java.math.RoundingMode.DOWN);
        result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      }
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_SET_SCALE_TRUNCATE.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale),
            new com.dremio.common.expression.ValueExpressions.IntExpression(scale.value)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"truncate", "trunc"}, scope = FunctionScope.SIMPLE, nulls =
    NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalZeroScaleTruncate.class)
  public static class TruncateDecimal implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, 0, java.math.RoundingMode.DOWN);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_ZERO_SCALE_TRUNCATE.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "ceil", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalZeroScale.class)
  public static class CeilDecimal implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, 0, java.math.RoundingMode.CEILING);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_ZERO_SCALE.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "floor", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL, derivation = OutputDerivation.DecimalZeroScale.class)
  public static class FloorDecimal implements SimpleFunction {

    @Param
    DecimalHolder inputHolder;
    @Output
    DecimalHolder resultHolder;
    @Inject
    ArrowBuf buffer;
    @Inject
    FunctionErrorContext functionErrorContext;


    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(16);
    }

    @Override
    public void eval() {
      long index = (inputHolder.start / (org.apache.arrow.vector.DecimalVector.TYPE_WIDTH));
      java.math.BigDecimal input = org.apache.arrow.vector.util.DecimalUtility.getBigDecimalFromArrowBuf(inputHolder.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(index), inputHolder.scale, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);

      java.math.BigDecimal result = com.dremio.exec.expr.fn.impl.DecimalFunctions.round(input, 0, java.math.RoundingMode.FLOOR);
      result = com.dremio.exec.expr.fn.impl.DecimalFunctions.checkOverflow(result);
      try {
        org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(result, buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      } catch (RuntimeException e) {
        throw functionErrorContext.error(e)
          .build();
      }
      resultHolder.buffer = buffer;
      com.dremio.common.expression.CompleteType outputType =
        com.dremio.exec.expr.fn.OutputDerivation.DECIMAL_ZERO_SCALE.getOutputType(
          com.dremio.common.expression.CompleteType.DECIMAL,
          java.util.Arrays.asList(
            new com.dremio.common.expression.ValueExpressions.DecimalExpression(input, inputHolder.precision, inputHolder.scale)));
      resultHolder.scale = outputType.getScale();
      resultHolder.precision = outputType.getPrecision();
    }
  }

}
