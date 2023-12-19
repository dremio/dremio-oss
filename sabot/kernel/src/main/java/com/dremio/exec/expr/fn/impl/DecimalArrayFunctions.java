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

import java.util.List;

import javax.inject.Inject;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableDecimalHolder;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import com.dremio.exec.expr.annotations.Workspace;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.base.Preconditions;

public class DecimalArrayFunctions {

  @FunctionTemplate(names = "array_sum_DECIMAL", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = DecimalArraySum.class)
  public static class ArraySumDECIMAL implements SimpleFunction {

    @Param
    private FieldReader in;
    @Workspace
    private NullableDecimalHolder sum;
    @Output
    private NullableDecimalHolder out;
    @Inject
    private ArrowBuf buffer;
    @Inject
    private FunctionErrorContext errCtx;

    @Override
    public void setup() {
    }
    @Override
    public void eval() {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_sum must be a LIST. Was given: %s",
            in.getMinorType().toString()
          )
        );
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.DECIMAL)) {
        throw new UnsupportedOperationException(
          String.format("List of %s is not comparable with %s",
            in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.DECIMAL
          )
        );
      }
      sum = new NullableDecimalHolder();
      sum.isSet = 1;
      buffer = buffer.reallocIfNeeded(16);
      sum.buffer = buffer;
      java.math.BigDecimal zero = new java.math.BigDecimal(java.math.BigInteger.ZERO, 0);
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(zero, sum.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      sum.start = 0;
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      while (listReader.next()){
        if (listReader.reader().readObject() != null){
          NullableDecimalHolder element = new NullableDecimalHolder();
          listReader.reader().read(element);
          com.dremio.exec.util.DecimalUtils.addSignedDecimalInLittleEndianBytes(sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start), element.buffer,
            org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(element.start), sum.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(sum.start));
        }
      }
      out.isSet = 1;
      out.buffer = sum.buffer;
      out.start = sum.start;
    }
  }

  @FunctionTemplate(names = "array_max_DECIMAL", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = DecimalArrayMinMax.class)
  public static class ArrayMaxDECIMAL implements SimpleFunction {

    @Param
    private FieldReader in;
    @Workspace
    private NullableDecimalHolder maxVal;
    @Output
    private NullableDecimalHolder out;
    @Inject
    private ArrowBuf buffer;
    @Inject
    private FunctionErrorContext errCtx;
    @Override
    public void setup () {
    }

    @Override
    public void eval () {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_max must be a LIST. Was given: %s", in.getMinorType().toString()));
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.DECIMAL)){
        throw new UnsupportedOperationException(String.format("List of %s is not comparable with %s",
          in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.DECIMAL));
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      if (listReader.size() == 0) {
        out.isSet = 0;
        return;
      }
      maxVal = new NullableDecimalHolder();
      maxVal.isSet = 1;
      maxVal.start = 0;
      buffer = buffer.reallocIfNeeded(16);
      maxVal.buffer = buffer;
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MIN_DECIMAL, maxVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      while (listReader.next()){
        if (listReader.reader().readObject() == null){
          out.isSet = 0;
          return;
        }
        NullableDecimalHolder element = new NullableDecimalHolder();
        listReader.reader().read(element);
        int compare = com.dremio.exec.util.DecimalUtils
          .compareSignedDecimalInLittleEndianBytes(element.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(element.start), maxVal.buffer, 0);
        if (compare > 0) {
          element.buffer.getBytes(element.start, maxVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        }
      }
      out.isSet = 1;
      out.buffer = maxVal.buffer;
      out.start = 0;
    }
  }

  @FunctionTemplate(names = "array_min_DECIMAL", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL, derivation = DecimalArrayMinMax.class)
  public static class ArrayMinDECIMAL implements SimpleFunction {

    @Param private FieldReader in;
    @Workspace
    private NullableDecimalHolder minVal;
    @Output
    private NullableDecimalHolder out;
    @Inject
    private ArrowBuf buffer;
    @Inject
    private FunctionErrorContext errCtx;

    @Override
    public void setup () {
    }

    @Override
    public void eval () {
      if (!in.isSet() || in.readObject() == null) {
        out.isSet = 0;
        return;
      }
      if (in.getMinorType() != org.apache.arrow.vector.types.Types.MinorType.LIST) {
        throw new UnsupportedOperationException(
          String.format("First parameter to array_min must be a LIST. Was given: %s", in.getMinorType().toString()));
      }
      if (!in.reader().getMinorType().equals(org.apache.arrow.vector.types.Types.MinorType.DECIMAL)){
        throw new UnsupportedOperationException(String.format("List of %s is not comparable with %s",
          in.reader().getMinorType().toString(), org.apache.arrow.vector.types.Types.MinorType.DECIMAL));
      }
      org.apache.arrow.vector.complex.impl.UnionListReader listReader = (org.apache.arrow.vector.complex.impl.UnionListReader) in;
      if (listReader.size() == 0) {
        out.isSet = 0;
        return;
      }
      minVal = new NullableDecimalHolder();
      minVal.isSet = 1;
      minVal.start = 0;
      buffer = buffer.reallocIfNeeded(16);
      minVal.buffer = buffer;
      org.apache.arrow.vector.util.DecimalUtility.writeBigDecimalToArrowBuf(com.dremio.exec.util.DecimalUtils.MAX_DECIMAL, minVal.buffer, 0, org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
      while (listReader.next()){
        if (listReader.reader().readObject() == null){
          out.isSet = 0;
          return;
        }
        NullableDecimalHolder element = new NullableDecimalHolder();
        listReader.reader().read(element);
        int compare = com.dremio.exec.util.DecimalUtils
          .compareSignedDecimalInLittleEndianBytes(element.buffer, org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(element.start), minVal.buffer, 0);
        if (compare < 0) {
          element.buffer.getBytes(element.start, minVal.buffer, 0 , org.apache.arrow.vector.DecimalVector.TYPE_WIDTH);
        }
      }
      out.isSet = 1;
      out.buffer = minVal.buffer;
      out.start = 0;
    }
  }

  public static class DecimalArraySum implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalArraySum.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1);
      CompleteType type = args.get(0).getCompleteType();
      if (!type.isList()) {
        throw UserException.functionError()
          .message("First parameter to function must be a LIST. Was given: %s",
            type.toString())
          .build();
      }
      CompleteType childType = type.getOnlyChildType();
      if(!childType.isDecimal()) {
        throw UserException.functionError()
          .message("List of %s is not comparable with %s",
            childType.toString(), org.apache.arrow.vector.types.Types.MinorType.DECIMAL)
          .build(logger);
      }

      return CompleteType.fromDecimalPrecisionScale(38, childType.getScale());
    }
  }

  public static class DecimalArrayMinMax implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalArrayMinMax.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      Preconditions.checkArgument(args.size() == 1);
      CompleteType type = args.get(0).getCompleteType();
      if (!type.isList()) {
        throw UserException.functionError()
          .message("First parameter to function must be a LIST. Was given: %s",
            type.toString())
          .build();
      }
      CompleteType childType = type.getOnlyChildType();
      if(!childType.isDecimal()) {
        throw UserException.functionError()
          .message("List of %s is not comparable with %s",
            childType.toString(), org.apache.arrow.vector.types.Types.MinorType.DECIMAL)
          .build(logger);
      }
      return CompleteType.fromDecimalPrecisionScale(childType.getPrecision(), childType.getScale());
    }
  }
}
