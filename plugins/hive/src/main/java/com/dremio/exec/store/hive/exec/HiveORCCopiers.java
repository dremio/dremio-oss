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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
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
      copiers[i] = createCopier(output[i], cols[projectedColOrdinals.get(i)]);
    }

    return copiers;
  }

  private static ORCCopier createCopier(ValueVector output, ColumnVector input) {
    if (output instanceof BaseVariableWidthVector) {
      return new VarWidthCopier((BytesColumnVector) input, (BaseVariableWidthVector) output);
    } else if (output instanceof IntVector) {
      return new IntCopier((LongColumnVector) input, (IntVector) output);
    } else if (output instanceof BigIntVector) {
      return new BigIntCopier((LongColumnVector) input, (BigIntVector) output);
    } else if (output instanceof Float4Vector) {
      return new Float4Copier((DoubleColumnVector) input, (Float4Vector) output);
    } else if (output instanceof Float8Vector) {
      return new Float8Copier((DoubleColumnVector) input, (Float8Vector) output);
    } else if (output instanceof DateMilliVector) {
      return new DateMilliCopier((LongColumnVector) input, (DateMilliVector) output);
    } else if (output instanceof TimeStampMilliVector) {
      return new TimeStampMilliCopier((TimestampColumnVector) input, (TimeStampMilliVector) output);
    } else if (output instanceof DecimalVector) {
      return new DecimalCopier((DecimalColumnVector) input, (DecimalVector) output);
    } else if (output instanceof BitVector) {
      return new BitCopier((LongColumnVector) input, (BitVector) output);
    }

    throw UserException.unsupportedError()
        .message("Received unsupported type '%s' in Hive vectorized ORC reader", output.getField())
        .build(logger);
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
        final boolean isNull[] = inputVector.isNull;
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
        final boolean isNull[] = inputVector.isNull;
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
        final boolean isNull[] = inputVector.isNull;
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
   // We can't rely noNulls flag due to the bug/hive shenanigans here (they set the noNulls flag wrong):
   // https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/RecordReaderImpl.java#L681
   // } else if (inputVector.noNulls) {
   //   for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
   //     outputVector.set(outputIdx, input[inputIdx]);
   //   }
      } else {
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx]);
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
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        // we can't just use unscaledValue() since BigDecimal doesn't store trailing zeroes
        // and we need to ensure decoding includes the correct scale.
        final byte[] value = input[0].getHiveDecimal().bigDecimalValue().movePointRight(scale).unscaledValue().toByteArray();
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setBigEndian(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final byte[] value = input[inputIdx].getHiveDecimal().bigDecimalValue().movePointRight(scale).unscaledValue().toByteArray();
          outputVector.setBigEndian(outputIdx, value);
        }
      } else {
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            byte[] v = input[inputIdx].getHiveDecimal().bigDecimalValue().movePointRight(scale).unscaledValue().toByteArray();
            outputVector.setBigEndian(outputIdx, v);
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
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.setSafe(outputIdx, (int) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class Float4Copier implements ORCCopier {
    private DoubleColumnVector inputVector;
    private Float4Vector outputVector;

    Float4Copier(DoubleColumnVector inputVector, Float4Vector outputVector) {
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
   // We can't rely noNulls flag due to the bug/hive shenanigans here (they set the noNulls flag wrong):
   // https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/RecordReaderImpl.java#L656
   // } else if (inputVector.noNulls) {
   //   for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
   //     outputVector.set(outputIdx, input[inputIdx]);
   //   }
      } else {
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, (float) input[inputIdx]);
          }
        }
      }
    }
  }

  private static class Float8Copier implements ORCCopier {
    private DoubleColumnVector inputVector;
    private Float8Vector outputVector;

    Float8Copier(DoubleColumnVector inputVector, Float8Vector outputVector) {
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
   // We can't rely noNulls flag due to the bug/hive shenanigans here (they set the noNulls flag wrong):
   // https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/io/orc/RecordReaderImpl.java#L656
   // } else if (inputVector.noNulls) {
   //   for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
   //     outputVector.set(outputIdx, input[inputIdx]);
   //   }
      } else {
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx]);
          }
        }
      }
    }
  }

  private static class VarWidthCopier implements ORCCopier {
    private BytesColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    VarWidthCopier(BytesColumnVector inputVector, BaseVariableWidthVector outputVector) {
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
        final boolean isNull[] = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.setSafe(outputIdx, vector[inputIdx], start[inputIdx], length[inputIdx]);
          }
        }
      }
    }
  }
}
