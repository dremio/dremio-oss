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
import java.util.ArrayList;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.expr.fn.OutputDerivation;
import com.google.common.collect.Lists;

/**
 * Tests decimal truncate and round decimal behavior;
 */
public class DecimalOutputDerivationTests  {
  private final ArrowType.Decimal DECIMAL_5_3 = new ArrowType.Decimal(5,3, 128);
  private final ArrowType.Decimal DECIMAL_4_3 = new ArrowType.Decimal(4,3, 128);
  private final ArrowType.Decimal DECIMAL_2_0 = new ArrowType.Decimal(2,0, 128);
  private final ArrowType.Decimal DECIMAL_1_0 = new ArrowType.Decimal(1,0, 128);
  private final ArrowType.Decimal DECIMAL_9_0 = new ArrowType.Decimal(9,0, 128);
  private final ArrowType.Decimal DECIMAL_8_0 = new ArrowType.Decimal(8,0, 128);

  @Test
  public void testRoundWithoutScaleArg() {
    OutputDerivation.DecimalZeroScaleRound decimalZeroScaleRound =
      new OutputDerivation.DecimalZeroScaleRound();

    LogicalExpression expr_2_1 = new ValueExpressions.DecimalExpression(new BigDecimal("1.0"), 2,
      1);
    ArrayList<LogicalExpression> args = Lists.newArrayList();
    args.add(expr_2_1);
    verifyReturnType(decimalZeroScaleRound, DECIMAL_2_0, args);

    LogicalExpression expr_1_1 = new ValueExpressions.DecimalExpression(new BigDecimal("0.1"), 1,
      1);
    args = Lists.newArrayList();
    args.add(expr_1_1);
    verifyReturnType(decimalZeroScaleRound, DECIMAL_1_0, args);
  }

  @Test
  public void testRoundWithScaleArg() {
    OutputDerivation.DecimalSetScaleRound decimalScaleRound =
      new OutputDerivation.DecimalSetScaleRound();
    ArrayList<LogicalExpression> args;

    LogicalExpression expr_5_4 = new ValueExpressions.DecimalExpression(new BigDecimal("1.2345"), 5,
      4);
    args = getInputArgs(expr_5_4, 3);
    verifyReturnType(decimalScaleRound, DECIMAL_5_3, args);

    args = getInputArgs(expr_5_4, -3);
    verifyReturnType(decimalScaleRound, DECIMAL_2_0, args);

    LogicalExpression expr_10_2 = new ValueExpressions.DecimalExpression(new BigDecimal("12345678.12")
      , 10, 2);
    args = getInputArgs(expr_10_2, -3);
    verifyReturnType(decimalScaleRound, DECIMAL_9_0, args);

    args = getInputArgs(expr_10_2, -12);
    verifyReturnType(decimalScaleRound, DECIMAL_9_0, args);
  }

  @Test
  public void testTruncateWithoutScaleArg() {
    OutputDerivation.DecimalZeroScaleTruncate decimalZeroScaleTruncate =
      new OutputDerivation.DecimalZeroScaleTruncate();

    LogicalExpression expr_2_1 = new ValueExpressions.DecimalExpression(new BigDecimal("1.0"), 2,
      1);
    ArrayList<LogicalExpression> args = Lists.newArrayList();
    args.add(expr_2_1);
    verifyReturnType(decimalZeroScaleTruncate, DECIMAL_1_0, args);

    LogicalExpression expr_1_1 = new ValueExpressions.DecimalExpression(new BigDecimal("0.1"), 1,
      1);
    args = Lists.newArrayList();
    args.add(expr_1_1);
    verifyReturnType(decimalZeroScaleTruncate, DECIMAL_1_0, args);
  }

  @Test
  public void testTruncateWithScaleArg() {
    OutputDerivation.DecimalSetScaleTruncate decimalSetScaleTruncate =
      new OutputDerivation.DecimalSetScaleTruncate();
    ArrayList<LogicalExpression> args;

    LogicalExpression expr_5_4 = new ValueExpressions.DecimalExpression(new BigDecimal("1.2345"), 5,
      4);
    args = getInputArgs(expr_5_4, 3);
    verifyReturnType(decimalSetScaleTruncate, DECIMAL_4_3, args);

    args = getInputArgs(expr_5_4, -3);
    verifyReturnType(decimalSetScaleTruncate, DECIMAL_1_0, args);

    LogicalExpression expr_10_2 = new ValueExpressions.DecimalExpression(new BigDecimal("12345678.12")
      , 10, 2);
    args = getInputArgs(expr_10_2, -3);
    verifyReturnType(decimalSetScaleTruncate, DECIMAL_8_0, args);

    args = getInputArgs(expr_10_2, -12);
    verifyReturnType(decimalSetScaleTruncate, DECIMAL_8_0, args);
  }

  @NotNull
  private ArrayList<LogicalExpression> getInputArgs(LogicalExpression expr_5_4, int i) {
    ArrayList<LogicalExpression> args;
    args = Lists.newArrayList();
    args.add(expr_5_4);
    args.add(new ValueExpressions.IntExpression(i));
    return args;
  }

  private void verifyReturnType(OutputDerivation decimalZeroScaleRound,
                                ArrowType.Decimal expectedOutputType,
                                ArrayList<LogicalExpression> args) {
    CompleteType decimal10 = decimalZeroScaleRound.getOutputType(null , args);
    Assert.assertTrue(decimal10.isDecimal());
    Assert.assertTrue(expectedOutputType.equals(decimal10.getType()));
  }

}
