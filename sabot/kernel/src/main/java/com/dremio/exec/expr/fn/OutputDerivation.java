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
package com.dremio.exec.expr.fn;

import static com.dremio.exec.expr.fn.DerivationShortcuts.prec;
import static com.dremio.exec.expr.fn.DerivationShortcuts.scale;
import static com.dremio.exec.expr.fn.DerivationShortcuts.val;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.dremio.common.util.CoreDecimalUtility;

public interface OutputDerivation {
  CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args);


  class Dummy implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      throw new UnsupportedOperationException();
    }
  }

  public static class Default implements OutputDerivation{
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      return baseReturn;
    }
  };


  public static class DecimalAdd implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalCast.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to add a decimal value with inccorect number of arguments. Expected two but received %d arguments.", args.size()).build(logger);
      }

      LogicalExpression left = args.get(0);
      LogicalExpression right = args.get(1);

      // compute the output scale and precision here
      int outputScale = Math.max(scale(left), scale(right));
      int maxResultIntegerDigits = Math.max((prec(left) - scale(left)), (prec(right) - scale(right))) + 1;

      int outputPrecision = (outputScale + maxResultIntegerDigits);

      // If we are beyond the maximum precision range, cut down the fractional part
      if (outputPrecision > 38) {
        outputPrecision = 38;
        outputScale = (outputPrecision - maxResultIntegerDigits >= 0) ? (outputPrecision - maxResultIntegerDigits) : 0;
      }

      return CompleteType.fromDecimalPrecisionScale(outputPrecision, outputScale);
    }
  };

  public static class DecimalMax implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      int scale = 0;
      int precision = 0;

      // Get the max scale and precision from the inputs
      for (LogicalExpression e : args) {
        Decimal arg = e.getCompleteType().getType(Decimal.class);
        scale = Math.max(scale, arg.getScale());
        precision = Math.max(precision, arg.getPrecision());
      }

      return CompleteType.fromDecimalPrecisionScale(precision, scale);
    }
  };

  public static class DecimalCast implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalCast.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 3 || !(args.get(1) instanceof LongExpression) || !(args.get(2) instanceof LongExpression) ) {
        throw UserException.functionError().message("Attempted to cast decimal function with inccorect number of arguments. Expected 3 arguments (value, scale, precision) but received %d arguments.", args.size()).build(logger);
      }

      int precision = (int) ((LongExpression)(args.get(1))).getLong();
      int scale = (int) ((LongExpression)(args.get(2))).getLong();
      return CompleteType.fromDecimalPrecisionScale(precision, scale);
    }
  };


  /*
   * Here we compute the scale and precision of the output decimal data type
   * based on the input scale and precision. Since division operation can be
   * a multiplication operation we compute the scale to be the sum of the inputs.
   * Eg: Input1 : precision = 5, scale = 3 ==> max integer digits = 2
   *     Input2 : precision = 7, scale = 4 ==> max integer digits = 3
   *
   *     Output: max integer digits ==> 2 (left integer digits) + 4 (right scale, when divide results in multiplication)
   *             max scale          ==> 3 + 4 = 7
   *
   *             Minimum precision required ==> 6 + 7 = 13
   *
   * Since our minimum precision required is 13, we will use DECIMAL18 as the output type
   * but since this is divide we will grant the remaining digits in DECIMAL18 to scale
   * so we have the following
   *    output scale      ==> 7 + (18 - 13) = 12
   *    output precision  ==> 18
   */
  public static class DecimalDivScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {

      assert args.size() == 2;
      LogicalExpression left = args.get(0);
      LogicalExpression right = args.get(1);

      // compute the output scale and precision here
      int outputScale = scale(left) + scale(right);
      int leftIntegerDigits = prec(left) - scale(left);
      int maxResultIntegerDigits = leftIntegerDigits + scale(right);


      int outputPrecision = CoreDecimalUtility.getPrecisionRange(outputScale + maxResultIntegerDigits);

      // Output precision should be greater or equal to the input precision
      outputPrecision = Math.max(outputPrecision, Math.max(prec(left), prec(right)));

      // Try and increase the scale if we have any room
      outputScale = (outputPrecision - maxResultIntegerDigits >= 0) ? (outputPrecision - maxResultIntegerDigits) : 0;

      return CompleteType.fromDecimalPrecisionScale(outputPrecision, outputScale);

    }
  };

  // DECIMAL_MAX_SCALE ==> DECIMAL MAX
  // DECIMAL_SUM_AGG ==> DECIMAL MAX

  /*
   * This function scope is used by divide functions for decimal data type.
   * DecimalScalePrecisionDivideFunction is used to compute the output types'
   * scale and precision
   */
  public static class DecimalModScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      assert args.size() == 2;
      LogicalExpression left = args.get(0);
      LogicalExpression right = args.get(1);

      // compute the output scale and precision here
      int outputScale = Math.max(scale(left), scale(right));
      int leftIntegerDigits = prec(left) - prec(right);

      int outputPrecision = CoreDecimalUtility.getPrecisionRange(outputScale + leftIntegerDigits);

      if (outputScale + leftIntegerDigits > outputPrecision) {
        outputScale = outputPrecision - leftIntegerDigits;
      }

      // Output precision should atleast be greater or equal to the input precision
      outputPrecision = Math.max(outputPrecision, Math.max(prec(left), prec(right)));

      return CompleteType.fromDecimalPrecisionScale(outputPrecision, outputScale);
    }
  };

  /* Used by functions like round, truncate which specify the scale for
   * the output as the second argument
   */
  public static class DecimalSetScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      assert (args.size() == 2) && (args.get(1) instanceof ValueExpressions.IntExpression);


      // Get the scale from the second argument which should be a constant
      final int scale = val(args.get(1));

      int precision = 0;
      for (LogicalExpression e : args) {
        precision = Math.max(precision, prec(e));
      }

      return CompleteType.fromDecimalPrecisionScale(precision, scale);
    }
  };

  /*
   * Here we compute the output scale and precision of the multiply function.
   * We simply add the input scale and precision to determine the output's scale
   * and precision
   */
  public static class DecimalSumScale implements OutputDerivation {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalSumScale.class);
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to cast decimal function with inccorect number of arguments. Expected 3 arguments (value, scale, precision) but received %d arguments.", args.size()).build(logger);
      }
      LogicalExpression left = args.get(0);
      LogicalExpression right = args.get(1);

      int outputScale = scale(left) + scale(right);
      int integerDigits = (prec(left) - scale(left)) + (prec(right) - scale(right));

      int outputPrecision = integerDigits + outputScale;

      // If we are beyond the maximum precision range, cut down the fractional part
      if (outputPrecision > 38) {
        outputPrecision = 38;
        outputScale = (outputPrecision - integerDigits >= 0) ? (outputPrecision - integerDigits) : 0;
      }

      return CompleteType.fromDecimalPrecisionScale(outputPrecision, outputScale);
    }
  };



  /*
   * This function scope is used when we need to remove the scale part.
   * trunc and round functions with single argument use this
   */
  public static class DecimalZeroScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      int precision = 0;
      for (LogicalExpression e : args) {
        precision = Math.max(precision, prec(e));
      }
      return CompleteType.fromDecimalPrecisionScale(precision, 0);
    }
  };

  /**
   * Used to derive the output precision and scale for sum and sum0 aggregations.
   * Rules follow T-SQL derivation rules.
   */
  public static class DecimalAggSum implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalCast.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 1) {
        throw UserException.functionError().message("Attempted to sum a decimal column with " +
          "inccorect number of arguments. Expected one but received %d arguments.", args.size()).build(logger);
      }

      LogicalExpression aggColumn = args.get(0);

      return CompleteType.fromDecimalPrecisionScale(38, aggColumn.getCompleteType().getScale());
    }
  };

  /**
   * Used to derive the output precision and scale for min and max aggregations.
   * Rules follow T-SQL derivation rules.
   */
  public static class DecimalAggMinMax implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalCast.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 1) {
        throw UserException.functionError().message("Attempted to min/max a decimal column with " +
          "inccorect number of arguments. Expected one but received %d arguments.", args.size()).build(logger);
      }

      LogicalExpression aggColumn = args.get(0);

      return CompleteType.fromDecimalPrecisionScale(aggColumn.getCompleteType().getPrecision(), aggColumn
        .getCompleteType().getScale());
    }
  };
}
