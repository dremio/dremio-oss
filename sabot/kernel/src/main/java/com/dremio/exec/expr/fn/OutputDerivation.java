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
package com.dremio.exec.expr.fn;

import static com.dremio.exec.expr.fn.DerivationShortcuts.prec;
import static com.dremio.exec.expr.fn.DerivationShortcuts.scale;
import static com.dremio.exec.expr.fn.DerivationShortcuts.val;

import java.util.List;

import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil;
import org.apache.arrow.gandiva.evaluator.DecimalTypeUtil.OperationType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.ValueExpressions.LongExpression;
import com.google.common.base.Preconditions;

public interface OutputDerivation {
  OutputDerivation DECIMAL_MAX = new DecimalMax();
  OutputDerivation DECIMAL_ZERO_SCALE = new DecimalZeroScale();
  OutputDerivation DECIMAL_ZERO_SCALE_ROUND = new DecimalZeroScaleRound();
  OutputDerivation DECIMAL_ZERO_SCALE_TRUNCATE = new DecimalZeroScaleTruncate();
  OutputDerivation DECIMAL_SET_SCALE_ROUND = new DecimalSetScaleRound();
  OutputDerivation DECIMAL_SET_SCALE_TRUNCATE = new DecimalSetScaleTruncate();
  OutputDerivation DECIMAL_UNION = new DecimalUnion();
  OutputDerivation DECIMAL_CAST = new DecimalCast();

  OutputDerivation DECIMAL_ADD = new DecimalAdd();
  OutputDerivation DECIMAL_SUBTRACT = new DecimalSubtract();
  OutputDerivation DECIMAL_MULTIPLY = new DecimalMultiply();
  OutputDerivation DECIMAL_DIVIDE = new DecimalDivide();
  OutputDerivation DECIMAL_NEGATIVE = new DecimalNegativeScale();
  OutputDerivation DECIMAL_MOD =
    ((base, args) -> DecimalGandivaBinaryOutput.getOutputType(OperationType.MOD, args));
  OutputDerivation DECIMAL_SINGLE_VALUE = new DecimalSingleValue();


  CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args);


  class Dummy implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      throw new UnsupportedOperationException();
    }
  }

  class Default implements OutputDerivation{
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      return baseReturn;
    }
  }

  class DecimalMax implements OutputDerivation {
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
  }

  class DecimalUnion implements OutputDerivation {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalUnion.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      int scale = 0;
      int integral = 0;

      // Get the max scale and integral part from the inputs
      for (LogicalExpression e : args) {
        Decimal arg = e.getCompleteType().getType(Decimal.class);
        scale = Math.max(scale, arg.getScale());
        integral = Math.max(integral, arg.getPrecision() - scale);
      }
      if (scale + integral > 38) {
        throw UserException.functionError().message(
          "derived precision for union of %d arguments exceeds 38", args.size()
        ).build(logger);
      }

      return CompleteType.fromDecimalPrecisionScale(integral + scale, scale);
    }
  }

  class DecimalCast implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalCast.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 3 || !(args.get(1) instanceof LongExpression) || !(args.get(2) instanceof LongExpression) ) {
        throw UserException.functionError().message("Attempted to cast decimal function with incorrect number of arguments. Expected 3 arguments (value, precision, scale) but received %d arguments.", args.size()).build(logger);
      }

      int precision = (int) ((LongExpression)(args.get(1))).getLong();
      int scale = (int) ((LongExpression)(args.get(2))).getLong();
      return CompleteType.fromDecimalPrecisionScale(precision, scale);
    }
  }

  /* Used by functions like round, truncate which specify the scale for
   * the output as the second argument
   */
  class DecimalSetScaleTruncate implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      assert (args.size() == 2) && (args.get(1) instanceof ValueExpressions.IntExpression);
      // Get the scale from the second argument which should be a constant
      int scale = val(args.get(1));
      ArrowType.Decimal type =  getDecimalOutputTypeForTruncate(prec(args.get(0)),
        scale(args.get(0)), scale);
      return CompleteType.fromDecimalPrecisionScale(type.getPrecision(), type.getScale());
    }
  }

  class DecimalSetScaleRound implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      assert (args.size() == 2) && (args.get(1) instanceof ValueExpressions.IntExpression);
      // Get the scale from the second argument which should be a constant
      int scale = val(args.get(1));
      ArrowType.Decimal type =  getDecimalOutputTypeForRound(prec(args.get(0)),
        scale(args.get(0)), scale);
      return CompleteType.fromDecimalPrecisionScale(type.getPrecision(), type.getScale());
    }
  }

  class DecimalZeroScaleRound implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      ArrowType.Decimal type =  getDecimalOutputTypeForRound(prec(args.get(0)),
        scale(args.get(0)), 0);
      return CompleteType.fromDecimalPrecisionScale(type.getPrecision(), type.getScale());
    }
  }

  class DecimalNegativeScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      return CompleteType.fromDecimalPrecisionScale(prec(args.get(0)), scale(args.get(0)));
    }
  }

  class DecimalZeroScaleTruncate implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      ArrowType.Decimal type =  getDecimalOutputTypeForTruncate(prec(args.get(0)),
        scale(args.get(0)), 0);
      return CompleteType.fromDecimalPrecisionScale(type.getPrecision(), type.getScale());
    }
  }

  static ArrowType.Decimal getDecimalOutputTypeForTruncate(int arg1Precision, int arg1Scale,
                                                           int destinationScale) {
    int finalScale = Math.min(arg1Scale, destinationScale);
    if (finalScale < 0) {
      // -ve scale has special semantics in round/truncate fns.
      finalScale = 0;
    }
    Preconditions.checkState(arg1Scale >= finalScale,
      "Final scale " + finalScale+" cannot be greater than " +
      "original scale of argument : " + arg1Scale);

    int finalPrecision = arg1Precision - (arg1Scale - finalScale);
    if (finalPrecision == 0) {
      finalPrecision = 1;
    }
    return new ArrowType.Decimal(finalPrecision, finalScale, 128);
  }

  static ArrowType.Decimal getDecimalOutputTypeForRound(int arg1Precision, int arg1Scale,
                                                        int destinationScale) {
    int finalScale = Math.min(arg1Scale, destinationScale);
    if (finalScale < 0) {
      // -ve scale has special semantics in round/truncate fns.
      finalScale = 0;
    }
    Preconditions.checkState(arg1Scale >= finalScale,
      "Final scale " + finalScale+" cannot be greater than " +
        "original scale of argument : " + arg1Scale);

    int finalPrecision = arg1Precision - (arg1Scale - finalScale);
    if (finalScale < arg1Scale) {
      finalPrecision++;
    }

    return new ArrowType.Decimal(finalPrecision, finalScale, 128);
  }

  /*
   * This function scope is used when we need to remove the scale part.
   * trunc and round functions with single argument use this
   */
  class DecimalZeroScale implements OutputDerivation {
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      int precision = 0;
      for (LogicalExpression e : args) {
        precision = Math.max(precision, prec(e));
      }
      return CompleteType.fromDecimalPrecisionScale(precision, 0);
    }
  }

  // Binary functions implemented in gandiva.
  class DecimalGandivaBinaryOutput {
    public static CompleteType getOutputType(OperationType operationType, List<LogicalExpression> args) {
      return new CompleteType(DecimalTypeUtil.getResultTypeForOperation(operationType,
        args.get(0).getCompleteType().getType(ArrowType.Decimal.class),
        args.get(1).getCompleteType().getType(ArrowType.Decimal.class)));
    }
  }

  /**
   * Used to derive the output precision and scale for sum and sum0 aggregations.
   * Rules follow T-SQL derivation rules.
   */
  class DecimalAggSum implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalAggSum.class);

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 1) {
        throw UserException.functionError().message("Attempted to sum a decimal column with " +
          "inccorect number of arguments. Expected one but received %d arguments.", args.size()).build(logger);
      }

      LogicalExpression aggColumn = args.get(0);

      return CompleteType.fromDecimalPrecisionScale(38, aggColumn.getCompleteType().getScale());
    }
  }

  /**
   * Used to derive the output precision and scale for min and max aggregations.
   * Rules follow T-SQL derivation rules.
   */
  class DecimalAggMinMax implements OutputDerivation {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DecimalAggMinMax.class);

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
  }

  class DecimalAdd implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to add decimal columns with \" +\n" +
          "          \"incorrect number of arguments. Expected two but received %d arguments.", args.size()).buildSilently();
      }
      return DecimalGandivaBinaryOutput.getOutputType(OperationType.ADD, args);
    }
  }

  class DecimalSingleValue implements OutputDerivation {

    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 1) {
        throw UserException.functionError().message("Attempted to get decimal value with \" +\n" +
          "          \"incorrect number of arguments. Expected one but received %d arguments.", args.size()).buildSilently();
      }
      return args.get(0).getCompleteType();
    }
  }

  class DecimalSubtract implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to subtract decimal columns with incorrect number of arguments. Expected two but received %d arguments.", args.size()).buildSilently();
      }
      return DecimalGandivaBinaryOutput.getOutputType(OperationType.SUBTRACT, args);
    }
  }

  class DecimalMultiply implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to multiply decimal columns with incorrect number of arguments. Expected two but received %d arguments.", args.size()).buildSilently();
      }
      return DecimalGandivaBinaryOutput.getOutputType(OperationType.MULTIPLY, args);
    }
  }

  class DecimalDivide implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to divide decimal columns with incorrect number of arguments. Expected two but received %d arguments.", args.size()).buildSilently();
      }
      return DecimalGandivaBinaryOutput.getOutputType(OperationType.DIVIDE, args);
    }
  }

  class DecimalMod implements OutputDerivation {
    @Override
    public CompleteType getOutputType(CompleteType baseReturn, List<LogicalExpression> args) {
      if (args.size() != 2) {
        throw UserException.functionError().message("Attempted to compute modulo on decimal columns with incorrect number of arguments. Expected two but received %d arguments.", args.size()).buildSilently();
      }
      return DecimalGandivaBinaryOutput.getOutputType(OperationType.MOD, args);
    }
  }

}
