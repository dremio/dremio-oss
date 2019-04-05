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
package com.dremio.exec.store.hive.exec;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.dremio.common.exceptions.UserException;

public class HiveORCCopiers {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveORCCopiers.class);

  /**
   * Copier interface that copies a set of records from ORC vector to Dremio vector.
   */
  public interface ORCCopier {
    /**
     * Copies given number of records starting at given location. It assumes that output vector
     * has enough memory or has ability to expand memory as needed.
     *
     * @param inputIdx index into the ORC vector
     * @param count how many to copy
     * @param outputIdx index into the Dremio vector
     */
    void copy(int inputIdx, int count, int outputIdx);
  }

  /**
   * Helper method to create {@link ORCCopier}s based on given input, output vector types and projected column ordinals.
   *
   * @param projectedColOrdinals ordinals of the columns that we are interested in reading from the file.
   * @param output
   * @param input
   * @return
   */
  public static ORCCopier[] createCopiers(final List<Integer> projectedColOrdinals, final ValueVector[] output,
      final VectorizedRowBatch input, boolean isOriginal) {
    final int numColumns = output.length;
    final ORCCopier[] copiers = new ORCCopier[numColumns];
    final ColumnVector[] cols = isOriginal ? input.cols : ((StructColumnVector) input.cols[HiveORCVectorizedReader.TRANS_ROW_COLUMN_INDEX]).fields;
    for (int i = 0; i < numColumns; i++) {
      boolean copierCreated = false;
      if (i < projectedColOrdinals.size()) {
        int projectedColOrdinal = projectedColOrdinals.get(i);
        if (projectedColOrdinal < cols.length) {
          copiers[i] = createCopier(output[i], cols[projectedColOrdinal]);
          copierCreated = true;
        }
      }
      if (!copierCreated) {
        copiers[i] = new NoOpCopier(null, null);
      }
    }
    return copiers;
  }

  private static ORCCopier createCopier(ValueVector output, ColumnVector input) {
    if (output instanceof BaseVariableWidthVector) {
      if (input instanceof  BytesColumnVector) {
        return new BytesToVarWidthCopier((BytesColumnVector) input, (BaseVariableWidthVector) output);
      } else if (input instanceof  DecimalColumnVector) {
        return new DecimalToVarWidthCopier((DecimalColumnVector) input, (BaseVariableWidthVector) output);
      } else if (input instanceof  DoubleColumnVector) {
        return new DoubleToVarWidthCopier((DoubleColumnVector) input, (BaseVariableWidthVector) output);
      } else if (input instanceof  LongColumnVector) {
        return new LongToVarWidthCopier((LongColumnVector) input, (BaseVariableWidthVector) output);
      } else if (input instanceof TimestampColumnVector) {
        return new TimestampToVarWidthCopier((TimestampColumnVector) input, (BaseVariableWidthVector) output);
      } else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof IntVector) {
      if (input instanceof  LongColumnVector) {
        return new IntCopier((LongColumnVector) input, (IntVector) output);
      } else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof BigIntVector) {
      if (input instanceof  LongColumnVector) {
        return new BigIntCopier((LongColumnVector) input, (BigIntVector) output);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof Float4Vector) {
      if (input instanceof  DoubleColumnVector) {
         return new DoubleToFloat4Copier((DoubleColumnVector) input, (Float4Vector) output);
      } else if (input instanceof  LongColumnVector) {
        return new LongToFloat4Copier((LongColumnVector) input, (Float4Vector) output);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof Float8Vector) {
      if (input instanceof  BytesColumnVector) {
        return new BytesToFloat8Copier((BytesColumnVector) input, (Float8Vector) output);
      } else if (input instanceof  DoubleColumnVector) {
        return new DoubleToFloat8Copier((DoubleColumnVector) input, (Float8Vector) output);
      } else if (input instanceof  LongColumnVector) {
        return new LongToFloat8Copier((LongColumnVector) input, (Float8Vector) output);
      } else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof DateMilliVector) {
      if (input instanceof  LongColumnVector) {
        return new DateMilliCopier((LongColumnVector) input, (DateMilliVector) output);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof TimeStampMilliVector) {
      if (input instanceof  TimestampColumnVector) {
        return new TimeStampMilliCopier((TimestampColumnVector) input, (TimeStampMilliVector) output);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof DecimalVector) {
      if (input instanceof  BytesColumnVector) {
        return new BytesToDecimalCopier((BytesColumnVector) input, (DecimalVector) output);
      } else if (input instanceof  DecimalColumnVector) {
        return new DecimalCopier((DecimalColumnVector) input, (DecimalVector) output);
      } else if (input instanceof  DoubleColumnVector) {
        return new DoubleToDecimalCopier((DoubleColumnVector) input, (DecimalVector) output);
      } else if (input instanceof  LongColumnVector) {
        return new LongToDecimalCopier((LongColumnVector) input, (DecimalVector) output);
      } else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof BitVector) {
      if (input instanceof  LongColumnVector) {
        return new BitCopier((LongColumnVector) input, (BitVector) output);
      }
      else {
        return new NoOpCopier(null, null);
      }
    }

    throw UserException.unsupportedError()
        .message("Received unsupported type '%s' in Hive vectorized ORC reader", output.getField())
        .build(logger);
  }

  private static class NoOpCopier implements ORCCopier{

    NoOpCopier(ColumnVector inputVector, ValueVector outputVector) {
    }

    public void copy(int inputIdx, int count, int outputIdx) {
    }
  }

  /**
   * General comments about all {@link ORCCopier} implementations:
   *
   * 1) We try to use "set" instead of "setSafe" for fixed width vectors to improve perf by avoiding realloc checks
   * 2) It assumes that there is enough space allocated for fixed width vectors and caller is expected to pass
   *    output ordinals in vectors that fall within the allocated space.
   * 3) There are 3 paths:
   *    i) Repeating: input vector has only one value repeated and the value is stored at 0th location. In this case
   *       we read the value once from input vector and write required number of times in output vector
   *    ii) No-nulls: We avoid checking for nulls in input vector
   *    iii) Non-repeating, has nulls: Before copying an element from input vector, we first check if the value is null
   *         in isNull array in input vector.
   */

  private static class IntCopier implements ORCCopier {
    private LongColumnVector inputVector;
    private IntVector outputVector;

    IntCopier(LongColumnVector inputVector, IntVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final int value = (int) input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, (int) input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, (int) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class BigIntCopier implements ORCCopier {
    private LongColumnVector inputVector;
    private BigIntVector outputVector;

    BigIntCopier(LongColumnVector inputVector, BigIntVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final long value = input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx]);
          }
        }
      }
    }
  }

  private static class DateMilliCopier implements ORCCopier {
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);

    private LongColumnVector inputVector;
    private DateMilliVector outputVector;

    DateMilliCopier(LongColumnVector inputVector, DateMilliVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      // Date is given as number of days, we store as millis
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final long value = input[0] * MILLIS_PER_DAY;
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, input[inputIdx] * MILLIS_PER_DAY);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx] * MILLIS_PER_DAY);
          }
        }
      }
    }
  }

  private static class TimeStampMilliCopier implements ORCCopier {
    private TimestampColumnVector inputVector;
    private TimeStampMilliVector outputVector;

    TimeStampMilliCopier(TimestampColumnVector inputVector, TimeStampMilliVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      // Input is in milliseconds since epoch and output is expected in same format
      final long[] input = inputVector.time;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final long value = input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx]);
          }
        }
      }
    }
  }

  private static class LongToDecimalCopier implements ORCCopier  {
    private LongColumnVector inputVector;
    private DecimalVector outputVector;

    LongToDecimalCopier(LongColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      final int outputPrecision = ((ArrowType.Decimal)outputVector.getField().getType()).getPrecision();
      final int outputScale = outputVector.getScale();
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        long value = input[0];
        HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(
          HiveDecimal.create(
            BigDecimal.valueOf(value).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
          outputPrecision, outputScale);
        if (hiveDecimal != null) {
          final byte[] decimalValue = hiveDecimal.bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
          for (int i = 0; i < count; i++, outputIdx++) {
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          try {
            final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
              HiveDecimal.create(
                BigDecimal.valueOf(input[inputIdx]).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
              outputPrecision, outputScale)
              .bigDecimalValue()
              .movePointRight(outputScale)
              .unscaledValue()
              .toByteArray();
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
          catch (Exception e) {
            // ignoring exception creates null entry
          }
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            try {
              final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
                HiveDecimal.create(
                  BigDecimal.valueOf(input[inputIdx]).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
                outputPrecision, outputScale)
                .bigDecimalValue()
                .movePointRight(outputScale)
                .unscaledValue()
                .toByteArray();
              outputVector.setBigEndian(outputIdx, decimalValue);
            }
            catch (Exception e) {
              // ignoring exception creates null entry
            }
          }
        }
      }
    }
  }

  private static class DoubleToDecimalCopier implements ORCCopier  {
    private DoubleColumnVector inputVector;
    private DecimalVector outputVector;

    DoubleToDecimalCopier(DoubleColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final double[] input = inputVector.vector;
      final int outputPrecision = ((ArrowType.Decimal)outputVector.getField().getType()).getPrecision();
      final int outputScale = outputVector.getScale();
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        double value = input[0];
        HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(
          HiveDecimal.create(
            BigDecimal.valueOf(value).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
          outputPrecision, outputScale);
        if (hiveDecimal != null) {
          final byte[] decimalValue = hiveDecimal.bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
          for (int i = 0; i < count; i++, outputIdx++) {
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          try {
            final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
              HiveDecimal.create(
                BigDecimal.valueOf(input[inputIdx]).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
              outputPrecision, outputScale)
              .bigDecimalValue()
              .movePointRight(outputScale)
              .unscaledValue()
              .toByteArray();
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
          catch (Exception e) {
            // ignoring exception creates null entry
          }
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            try {
              final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
                HiveDecimal.create(
                  BigDecimal.valueOf(input[inputIdx]).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
                outputPrecision, outputScale)
                .bigDecimalValue()
                .movePointRight(outputScale)
                .unscaledValue()
                .toByteArray();
              outputVector.setBigEndian(outputIdx, decimalValue);
            }
            catch (Exception e) {
              // ignoring exception creates null entry
            }
          }
        }
      }
    }
  }

  private static class BytesToDecimalCopier implements ORCCopier {
    private BytesColumnVector inputVector;
    private DecimalVector outputVector;

    BytesToDecimalCopier(BytesColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final byte[][] vector = inputVector.vector;
      final int[] start = inputVector.start;
      final int[] length = inputVector.length;
      final int outputPrecision = ((ArrowType.Decimal)outputVector.getField().getType()).getPrecision();
      final int outputScale = outputVector.getScale();
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        try {
          String strValue = new String(vector[0], start[0], length[0], StandardCharsets.UTF_8);
          HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(
            HiveDecimal.create(
              new BigDecimal(strValue).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
            outputPrecision, outputScale);
          if (hiveDecimal != null) {
            final byte[] decimalValue = hiveDecimal.bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
        } catch (Exception e) {

        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          try {
            String strValue = new String(vector[inputIdx], start[inputIdx], length[inputIdx], StandardCharsets.UTF_8);
            final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
              HiveDecimal.create(
                new BigDecimal(strValue).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
              outputPrecision, outputScale)
              .bigDecimalValue()
              .movePointRight(outputScale)
              .unscaledValue()
              .toByteArray();;
            outputVector.setBigEndian(outputIdx, decimalValue);
          }
          catch (Exception e) {

          }
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            try {
              String strValue = new String(vector[inputIdx], start[inputIdx], length[inputIdx], StandardCharsets.UTF_8);
              final byte[] decimalValue = HiveDecimal.enforcePrecisionScale(
                HiveDecimal.create(
                  new BigDecimal(strValue).setScale(outputVector.getScale(), RoundingMode.HALF_UP)),
                outputPrecision, outputScale)
                .bigDecimalValue()
                .movePointRight(outputScale)
                .unscaledValue()
                .toByteArray();;
              outputVector.setBigEndian(outputIdx, decimalValue);
            } catch (Exception e) {

            }
          }
        }
      }
    }
  }

  private static class DecimalCopier implements ORCCopier {
    private DecimalColumnVector inputVector;
    private DecimalVector outputVector;

    DecimalCopier(DecimalColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      // TODO: Decimal is not handled in an optimal way. Avoid creating byte arrays
      final HiveDecimalWritable[] input = inputVector.vector;
      final int scale = inputVector.scale;
      final int outputPrecision = ((ArrowType.Decimal)outputVector.getField().getType()).getPrecision();
      final int outputScale = outputVector.getScale();
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes
        // and we need to ensure decoding includes the correct scale.
        HiveDecimal hiveDecimal = HiveDecimal.enforcePrecisionScale(input[0].getHiveDecimal(), outputPrecision, outputScale);
        if (hiveDecimal != null) {
          final byte[] value = hiveDecimal.bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
          for (int i = 0; i < count; i++, outputIdx++) {
            outputVector.setBigEndian(outputIdx, value);
          }
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          try {
            final byte[] value = HiveDecimal.enforcePrecisionScale(input[inputIdx].getHiveDecimal(), outputPrecision, outputScale).bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
            outputVector.setBigEndian(outputIdx, value);
          }
          catch (Exception e) {
            // ignoring exception sets null.
            // enforcePrecisionScale returns null when it cannot enforce
          }
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            try {
              byte[] v = HiveDecimal.enforcePrecisionScale(input[inputIdx].getHiveDecimal(), outputPrecision, outputScale).bigDecimalValue().movePointRight(outputScale).unscaledValue().toByteArray();
              outputVector.setBigEndian(outputIdx, v);
            }
            catch (Exception e) {
              // ignoring exception sets null.
              // enforcePrecisionScale returns null when it cannot enforce
            }
          }
        }
      }
    }
  }

  private static class BitCopier implements ORCCopier {
    private LongColumnVector inputVector;
    private BitVector outputVector;

    BitCopier(LongColumnVector inputVector, BitVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final int value = (int) input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.setSafe(outputIdx, (int) input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.setSafe(outputIdx, (int) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class LongToFloat4Copier implements ORCCopier  {
    private LongColumnVector inputVector;
    private Float4Vector outputVector;

    LongToFloat4Copier(LongColumnVector inputVector, Float4Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final float value = (float)input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, (float)input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, (float) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class DoubleToFloat4Copier implements ORCCopier {
    private DoubleColumnVector inputVector;
    private Float4Vector outputVector;

    DoubleToFloat4Copier(DoubleColumnVector inputVector, Float4Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final double[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final float value = (float)input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, (float)input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, (float) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class BytesToFloat8Copier implements ORCCopier {
    private BytesColumnVector inputVector;
    private Float8Vector outputVector;

    BytesToFloat8Copier(BytesColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final byte[][] vector = inputVector.vector;
      final int[] start = inputVector.start;
      final int[] length = inputVector.length;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        try {
          String strValue = new String(vector[0], start[0], length[0], StandardCharsets.UTF_8);
          double doubleValue = Double.parseDouble(strValue);
          for (int i = 0; i < count; i++, outputIdx++) {
            outputVector.set(outputIdx, doubleValue);
          }
        } catch (Exception e) {
          return;
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (inputVector.noNulls || !isNull[inputIdx]) {
            try {
              String strValue = new String(vector[inputIdx], start[inputIdx], length[inputIdx], StandardCharsets.UTF_8);
              double doubleValue = Double.parseDouble(strValue);
              outputVector.set(outputIdx, doubleValue);
            }
            catch (Exception e) {

            }
          }
        }
      }
    }
  }

  private static class LongToFloat8Copier implements ORCCopier {
    private LongColumnVector inputVector;
    private Float8Vector outputVector;

    LongToFloat8Copier(LongColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final double value = (double)input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, (double)input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, (double)input[inputIdx]);
          }
        }
      }
    }
  }

  private static class DoubleToFloat8Copier implements ORCCopier {
    private DoubleColumnVector inputVector;
    private Float8Vector outputVector;

    DoubleToFloat8Copier(DoubleColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final double[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final double value = input[0];
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, input[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx]);
          }
        }
      }
    }
  }

  private static class LongToVarWidthCopier implements ORCCopier {
    private LongColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    LongToVarWidthCopier (LongColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      // We can hit this path if input is Date type in hive
      // For now, not converting date into human readable date format.
      // Will need to revisit if output for Date to String conversion is not what users want
      final long[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final long value = input[0];
        byte[] valuebytes = Long.toString(value).getBytes();

        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, valuebytes);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final long value = input[inputIdx];
          outputVector.setSafe(outputIdx, Long.toString(value).getBytes());
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            final long value = input[inputIdx];
            outputVector.setSafe(outputIdx, Long.toString(value).getBytes());
          }
        }
      }
    }
  }

  private static class TimestampToVarWidthCopier implements ORCCopier {
    private TimestampColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    TimestampToVarWidthCopier(TimestampColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      // For now, not converting timestamp to human readable date form
      // will need to revisit if output is not what users want
      final long[] input = inputVector.time;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final byte[] value = Long.toString(input[0]).getBytes();
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final byte[] value = Long.toString(input[inputIdx]).getBytes();
          outputVector.set(outputIdx, value);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            final byte[] value = Long.toString(input[inputIdx]).getBytes();
            outputVector.set(outputIdx, value);
          }
        }
      }
    }
  }

  private static class DoubleToVarWidthCopier implements ORCCopier {
    private DoubleColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    DoubleToVarWidthCopier(DoubleColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final double[] input = inputVector.vector;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final byte[] value = Double.toString(input[0]).getBytes();
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final byte[] value = Double.toString(input[inputIdx]).getBytes();
          outputVector.set(outputIdx, value);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            final byte[] value = Double.toString(input[inputIdx]).getBytes();
            outputVector.set(outputIdx, value);
          }
        }
      }
    }
  }

  private static class DecimalToVarWidthCopier implements ORCCopier {
    private DecimalColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    DecimalToVarWidthCopier(DecimalColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final HiveDecimalWritable[] input = inputVector.vector;
      final int scale = inputVector.scale;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes
        // and we need to ensure decoding includes the correct scale.
        final byte[] value = input[0].getHiveDecimal().bigDecimalValue().toString().getBytes();
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final byte[] value = input[inputIdx].getHiveDecimal().bigDecimalValue().toString().getBytes();
          outputVector.setSafe(outputIdx, value);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            byte[] v = input[inputIdx].getHiveDecimal().bigDecimalValue().toString().getBytes();
            outputVector.setSafe(outputIdx, v);
          }
        }
      }
    }
  }

  private static class BytesToVarWidthCopier implements ORCCopier {
    private BytesColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    BytesToVarWidthCopier(BytesColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      final byte[][] vector = inputVector.vector;
      final int[] start = inputVector.start;
      final int[] length = inputVector.length;

      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final byte[] value = new byte[length[0]];
        System.arraycopy(vector[0], start[0], value, 0, length[0]);
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.setSafe(outputIdx, vector[inputIdx], start[inputIdx], length[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.setSafe(outputIdx, vector[inputIdx], start[inputIdx], length[inputIdx]);
          }
        }
      }
    }
  }
}
