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
package com.dremio.exec.store.hive.exec;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.exec.store.hive.exec.HiveAbstractReader.HiveOperatorContextOptions;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import com.dremio.common.exceptions.UserException;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.annotations.VisibleForTesting;

public class HiveORCCopiers {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveORCCopiers.class);

  public static class HiveColumnVectorData {
    private boolean[] include;
    private int[] counts;
    public HiveColumnVectorData(final boolean[] include, final int[] counts) {
      this.include = include;
      this.counts = counts;
    }

    // returns true if this vector at index 'position' is included in
    // the list of columns read from ORC
    public boolean isColumnVectorIncluded(int position) {
      if (position >= this.include.length) {
        return false;
      }
      return this.include[position];
    }

    // returns the cumulative vector count of self and all children (recursively)
    // for the schema element at index 'position'
    public int getTotalVectorCount(int position) {
      if (position >= this.counts.length) {
        return 0;
      }
      return this.counts[position];
    }
  }
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
    void ensureHasRequiredCapacity(int required);
  }

  private static abstract class ORCCopierBase implements ORCCopier {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveORCCopiers.class);
    public abstract void copy(int inputIdx, int count, int outputIdx);
    public abstract void ensureHasRequiredCapacity(int required);
    protected void ensureVectorHasRequiredCapacity(ValueVector vector, int required) {
      while (required > vector.getValueCapacity()) {
        vector.reAlloc();
      }
      // setValueCount helps in setting inputBytes job stat correctly
      vector.setValueCount(required);
    }
  }

  /**
   * Helper method to create {@link ORCCopier}s based on given input, output vector types and projected column ordinals.
   *
   * @param projectedColOrdinals ordinals of the columns that we are interested in reading from the file.
   * @param output
   * @param input
   * @return
   */
  public static ORCCopier[] createCopiers(final HiveColumnVectorData columnVectorData,
                                          final List<Integer> projectedColOrdinals,
                                          int[] ordinalIdsFromOrcFile,
                                          final ValueVector[] output,
                                          final VectorizedRowBatch input,
                                          boolean isOriginal,
                                          HiveOperatorContextOptions operatorContextOptions) {
    final int numColumns = output.length;
    final ORCCopier[] copiers = new ORCCopier[numColumns];
    final ColumnVector[] cols = isOriginal ? input.cols : ((StructColumnVector) input.cols[HiveORCVectorizedReader.TRANS_ROW_COLUMN_INDEX]).fields;
    for (int i = 0; i < numColumns; i++) {
      boolean copierCreated = false;
      if (i < projectedColOrdinals.size()) {
        int projectedColOrdinal = projectedColOrdinals.get(i);
        if (projectedColOrdinal < ordinalIdsFromOrcFile.length && projectedColOrdinal < cols.length) {
          int ordinalId = ordinalIdsFromOrcFile[ projectedColOrdinal ];
          copiers[i] = createCopier(columnVectorData, ordinalId, output[i], cols[projectedColOrdinal], operatorContextOptions);
          copierCreated = true;
        }
      }
      if (!copierCreated) {
        copiers[i] = new NoOpCopier(null, null);
      }
    }
    return copiers;
  }

  /*
  include: whether a particular column or subfield is included or not
  childCounts: Cumulative number of vectors used by column or subfield
  ordinalId: position of vector in the
   */
  private static ORCCopier createCopier(HiveColumnVectorData columnVectorData,
                                        int ordinalId,
                                        ValueVector output,
                                        ColumnVector input,
                                        HiveOperatorContextOptions operatorContextOptions) {
    if (output instanceof BaseVariableWidthVector) {
      if (input instanceof  BytesColumnVector) {
        return new BytesToVarWidthCopier((BytesColumnVector) input, (BaseVariableWidthVector) output, operatorContextOptions);
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
    } else if (output instanceof ListVector) {
      if (input instanceof  MultiValuedColumnVector) {
        return new ListCopier(columnVectorData, ordinalId,
          (MultiValuedColumnVector) input, (ListVector) output, operatorContextOptions);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof StructVector) {
      if (input instanceof  StructColumnVector) {
        return new StructCopier(columnVectorData, ordinalId,
          (StructColumnVector) input, (StructVector) output, operatorContextOptions);
      }
      else {
        return new NoOpCopier(null, null);
      }
    } else if (output instanceof UnionVector) {
      if (input instanceof  UnionColumnVector) {
        return new UnionCopier(columnVectorData, ordinalId,
          (UnionColumnVector) input, (UnionVector) output, operatorContextOptions);
      }
      else {
        return new NoOpCopier(null, null);
      }
    }

    throw UserException.unsupportedError()
        .message("Received unsupported type '%s' in Hive vectorized ORC reader", output.getField())
        .build(logger);
  }

  private static class NoOpCopier extends ORCCopierBase {
    NoOpCopier(ColumnVector inputVector, ValueVector outputVector) {
    }
    @Override
    public void ensureHasRequiredCapacity(int required) {
    }
    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
    }
  }

  private static class UnionCopier extends ORCCopierBase {
    private UnionColumnVector inputVector;
    private UnionVector outputVector;
    ArrayList<ORCCopier> fieldCopiers = new ArrayList<>();
    ArrayList<ValueVector> arrowFieldVectors = new ArrayList<>();

    UnionCopier(HiveColumnVectorData columnVectorData,
                int ordinalId,
                UnionColumnVector inputVector,
                UnionVector outputVector,
                HiveOperatorContextOptions operatorContextOptions) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
      // The loop below assumes that the getChildrenFromFields() API returns
      // the list of children in the same order as was provided when building the UnionVector.
      List<FieldVector> childArrowFields = outputVector.getChildrenFromFields();
      int childPos = ordinalId + 1; // first field is immediately next to union vector itself
      for (int idx=0; idx<childArrowFields.size(); ++idx) {
        if (idx < inputVector.fields.length) {
          ColumnVector hiveFieldVector = inputVector.fields[idx];
          ValueVector arrowfieldVector = childArrowFields.get(idx);
          arrowFieldVectors.add(arrowfieldVector);
          ORCCopier childCopier = createCopier(columnVectorData, childPos, arrowfieldVector, hiveFieldVector, operatorContextOptions);
          fieldCopiers.add(childCopier);
          childPos += columnVectorData.getTotalVectorCount(childPos);
        } else {
          fieldCopiers.add(new NoOpCopier(null, null));
        }
      }
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
     super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      if (inputVector.noNulls) {
        for (int rowIndex = 0; rowIndex < count; rowIndex++) {
          outputVector.setType(outputIdx + rowIndex, arrowFieldVectors.get(inputVector.tags[rowIndex]).getMinorType());
        }
      } else {
        for (int rowIndex = 0; rowIndex < count; rowIndex++) {
          if (!inputVector.isNull[rowIndex]) {
            outputVector.setType(outputIdx + rowIndex, arrowFieldVectors.get(inputVector.tags[rowIndex]).getMinorType());
          }
        }
      }
      int fieldCount = inputVector.fields.length;
      for (int idx=0; idx<fieldCount; ++idx) {
        fieldCopiers.get(idx).copy(inputIdx, count, outputIdx);
      }
    }
  }

  private static class StructCopier extends ORCCopierBase {
    private StructColumnVector inputVector;
    private StructVector outputVector;
    ArrayList<ORCCopier> fieldCopiers = new ArrayList<>();

    StructCopier(HiveColumnVectorData columnVectorData,
                 int ordinalId,
                 StructColumnVector inputVector,
                 StructVector outputVector, HiveOperatorContextOptions operatorContextOptions) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;

      int fieldCount = inputVector.fields.length;
      int arrowIdx = 0;
      int childPos = ordinalId + 1; // first child is immediately next to struct vector itself

      for (int idx=0; idx<fieldCount; ++idx) {
        if (columnVectorData.isColumnVectorIncluded(childPos)) {
          ValueVector arrowElementVector = outputVector.getVectorById(arrowIdx);
          ColumnVector hiveElementVector = inputVector.fields[idx];
          ORCCopier childCopier = createCopier(columnVectorData, childPos,
            arrowElementVector, hiveElementVector, operatorContextOptions);
          fieldCopiers.add(childCopier);
          arrowIdx++;
        }
        else {
          fieldCopiers.add(new NoOpCopier(null, null));
        }
        childPos += columnVectorData.getTotalVectorCount(childPos);
      }
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      int fieldCount = inputVector.fields.length;
      for (int idx=0; idx<fieldCount; ++idx) {
        fieldCopiers.get(idx).copy(inputIdx, count, outputIdx);
      }

      if (inputVector.noNulls) {
        for (int rowIndex = 0; rowIndex < count; rowIndex++) {
          outputVector.setIndexDefined(outputIdx + rowIndex);
        }
      } else {
        for (int rowIndex = 0; rowIndex < count; rowIndex++) {
          if (inputVector.isNull[rowIndex]) {
            outputVector.setNull(outputIdx + rowIndex);
          } else {
            outputVector.setIndexDefined(outputIdx + rowIndex);
          }
        }
      }
    }
  }

  static class ListCopier  extends ORCCopierBase {
    private MultiValuedColumnVector inputVector;
    private ListVector outputVector;
    private ORCCopier childCopier;
    private int childOutputIdx;

    @VisibleForTesting
    ListCopier(MultiValuedColumnVector inputVector) {
      this.inputVector = inputVector;
    }

    ListCopier(HiveColumnVectorData columnVectorData,
               int ordinalId,
               MultiValuedColumnVector inputVector,
               ListVector outputVector, HiveOperatorContextOptions operatorContextOptions) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
      if (inputVector instanceof ListColumnVector) {
        int childPos = ordinalId + 1; // first child is immediately next to list vector itself
        ListColumnVector inputListColumnVector = (ListColumnVector)inputVector;
        final ColumnVector hiveElementVector = inputListColumnVector.child;
        final ValueVector arrowElementVector = outputVector.getDataVector();
        childCopier = createCopier(columnVectorData, childPos,
          arrowElementVector, hiveElementVector, operatorContextOptions);
      } else if (inputVector instanceof MapColumnVector) {
        // Convert input Map column vector to List of Structures
        int childPos = ordinalId; // in case of map, list vector is a wrapper so we continue from same ordinalId
        MapColumnVector inputMapColumnVector = (MapColumnVector)inputVector;
        final ColumnVector hiveElementVector= new StructColumnVector(VectorizedRowBatch.DEFAULT_SIZE,
          new ColumnVector[] {inputMapColumnVector.keys, inputMapColumnVector.values});
        final ValueVector arrowElementVector = outputVector.getDataVector();
        childCopier = createCopier(columnVectorData, ordinalId, arrowElementVector, hiveElementVector, operatorContextOptions);
      }
      this.childOutputIdx = 0;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @VisibleForTesting
    long countChildren(boolean noNulls, long[] lengths, int startIndex, int count) {
      long retCount = 0;
      if (noNulls) {
        for (int idx = 0; idx < count; ++idx) {
          retCount += lengths[startIndex + idx];
        }
      } else {
        for (int idx = 0; idx < count; ++idx) {
          if (!inputVector.isNull[startIndex + idx]) {
            retCount +=  lengths[startIndex + idx];
          }
        }
      }
      return retCount;
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      final ArrowBuf offsetBuffer = outputVector.getOffsetBuffer();
      int nextOffset = (outputIdx == 0) ? 0 : offsetBuffer.getInt(outputIdx * ListVector.OFFSET_WIDTH);

      //count the number of children that need to be skipped
      int childInputIdx = (int)countChildren(inputVector.noNulls, inputVector.lengths, 0, inputIdx);

      //count the number of children that need to be copied
      int childCount = (int)countChildren(inputVector.noNulls, inputVector.lengths, inputIdx, count);

      if (outputIdx == 0) {
        childOutputIdx = 0;
      }

      childCopier.copy(childInputIdx, childCount, childOutputIdx);
      childOutputIdx += childCount;

      for(int idx=0; idx<count; ++idx) {
        if (inputVector.isNull[inputIdx + idx]) {
          offsetBuffer.setInt((outputIdx + idx) * ListVector.OFFSET_WIDTH, nextOffset);
        } else {
          offsetBuffer.setInt((outputIdx + idx) * ListVector.OFFSET_WIDTH, nextOffset);
          nextOffset += (int)inputVector.lengths[inputIdx + idx];
          outputVector.setNotNull(outputIdx + idx);
        }
      }
      offsetBuffer.setInt((outputIdx + count) * ListVector.OFFSET_WIDTH, nextOffset);
    }
  }


  /**
   * General comments about all {@link ORCCopier} implementations:
   * 1) There are 3 paths:
   *    i) Repeating: input vector has only one value repeated and the value is stored at 0th location. In this case
   *       we read the value once from input vector and write required number of times in output vector
   *    ii) No-nulls: We avoid checking for nulls in input vector
   *    iii) Non-repeating, has nulls: Before copying an element from input vector, we first check if the value is null
   *         in isNull array in input vector.
   */

  private static class IntCopier  extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private IntVector outputVector;

    IntCopier(LongColumnVector inputVector, IntVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class BigIntCopier  extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private BigIntVector outputVector;

    BigIntCopier(LongColumnVector inputVector, BigIntVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class DateMilliCopier  extends ORCCopierBase  {
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);

    private LongColumnVector inputVector;
    private DateMilliVector outputVector;

    DateMilliCopier(LongColumnVector inputVector, DateMilliVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class TimeStampMilliCopier  extends ORCCopierBase  {
    private TimestampColumnVector inputVector;
    private TimeStampMilliVector outputVector;

    TimeStampMilliCopier(TimestampColumnVector inputVector, TimeStampMilliVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      // Input is in milliseconds since epoch and output is expected in same format
      final long[] input = inputVector.time;
      final int[] inputnanos = inputVector.nanos;
      final int NANO_TO_MILLIS = 1000000;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final long value = input[0] + (inputnanos[0] / NANO_TO_MILLIS);

        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.set(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          outputVector.set(outputIdx, input[inputIdx] + (inputnanos[inputIdx] / NANO_TO_MILLIS));
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            outputVector.set(outputIdx, input[inputIdx] + (inputnanos[inputIdx] / NANO_TO_MILLIS));
          }
        }
      }
    }
  }

  private static class LongToDecimalCopier extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private DecimalVector outputVector;

    LongToDecimalCopier(LongColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class DoubleToDecimalCopier extends ORCCopierBase  {
    private DoubleColumnVector inputVector;
    private DecimalVector outputVector;

    DoubleToDecimalCopier(DoubleColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class BytesToDecimalCopier extends ORCCopierBase  {
    private BytesColumnVector inputVector;
    private DecimalVector outputVector;

    BytesToDecimalCopier(BytesColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class DecimalCopier extends ORCCopierBase  {
    private DecimalColumnVector inputVector;
    private DecimalVector outputVector;

    DecimalCopier(DecimalColumnVector inputVector, DecimalVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class BitCopier  extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private BitVector outputVector;

    BitCopier(LongColumnVector inputVector, BitVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class LongToFloat4Copier extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private Float4Vector outputVector;

    LongToFloat4Copier(LongColumnVector inputVector, Float4Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class DoubleToFloat4Copier extends ORCCopierBase  {
    private DoubleColumnVector inputVector;
    private Float4Vector outputVector;

    DoubleToFloat4Copier(DoubleColumnVector inputVector, Float4Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class BytesToFloat8Copier extends ORCCopierBase  {
    private BytesColumnVector inputVector;
    private Float8Vector outputVector;

    BytesToFloat8Copier(BytesColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class LongToFloat8Copier extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private Float8Vector outputVector;

    LongToFloat8Copier(LongColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class DoubleToFloat8Copier extends ORCCopierBase  {
    private DoubleColumnVector inputVector;
    private Float8Vector outputVector;

    DoubleToFloat8Copier(DoubleColumnVector inputVector, Float8Vector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class LongToVarWidthCopier extends ORCCopierBase  {
    private LongColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    LongToVarWidthCopier (LongColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class TimestampToVarWidthCopier extends ORCCopierBase {
    private TimestampColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    TimestampToVarWidthCopier(TimestampColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      // For now, not converting timestamp to human readable date form
      // will need to revisit if output is not what users want
      final long[] input = inputVector.time;
      final int[] inputnanos = inputVector.nanos;
      final int NANO_TO_MILLIS = 1000000;
      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        final byte[] value = Long.toString(input[0] + (inputnanos[0] / NANO_TO_MILLIS)).getBytes();
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          final byte[] value = Long.toString(input[inputIdx] + (inputnanos[inputIdx] / NANO_TO_MILLIS)).getBytes();
          outputVector.setSafe(outputIdx, value);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            final byte[] value = Long.toString(input[inputIdx] + (inputnanos[inputIdx] / NANO_TO_MILLIS)).getBytes();
            outputVector.setSafe(outputIdx, value);
          }
        }
      }
    }
  }

  private static class DoubleToVarWidthCopier extends ORCCopierBase {
    private DoubleColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    DoubleToVarWidthCopier(DoubleColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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
          outputVector.setSafe(outputIdx, value);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            final byte[] value = Double.toString(input[inputIdx]).getBytes();
            outputVector.setSafe(outputIdx, value);
          }
        }
      }
    }
  }

  private static class DecimalToVarWidthCopier extends ORCCopierBase {
    private DecimalColumnVector inputVector;
    private BaseVariableWidthVector outputVector;

    DecimalToVarWidthCopier(DecimalColumnVector inputVector, BaseVariableWidthVector outputVector) {
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
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

  private static class BytesToVarWidthCopier extends ORCCopierBase  {
    private BytesColumnVector inputVector;
    private BaseVariableWidthVector outputVector;
    private HiveOperatorContextOptions operatorContextOptions;

    private void checkSizeLimit(int size) {
      FieldSizeLimitExceptionHelper.checkSizeLimit(size, this.operatorContextOptions.getMaxCellSize(), logger);
    }

    BytesToVarWidthCopier(BytesColumnVector inputVector, BaseVariableWidthVector outputVector, HiveOperatorContextOptions operatorContextOptions) {
      this.operatorContextOptions = operatorContextOptions;
      this.inputVector = inputVector;
      this.outputVector = outputVector;
    }

    @Override
    public void ensureHasRequiredCapacity(int required) {
      super.ensureVectorHasRequiredCapacity(this.outputVector, required);
    }

    @Override
    public void copy(int inputIdx, int count, int outputIdx) {
      ensureHasRequiredCapacity(outputIdx + count);
      final byte[][] vector = inputVector.vector;
      final int[] start = inputVector.start;
      final int[] length = inputVector.length;

      if (inputVector.isRepeating) {
        if (inputVector.isNull[0]) {
          return; // If all repeating values are null, then there is no need to write anything to vector
        }
        checkSizeLimit(length[0]);
        final byte[] value = new byte[length[0]];
        System.arraycopy(vector[0], start[0], value, 0, length[0]);
        for (int i = 0; i < count; i++, outputIdx++) {
          outputVector.setSafe(outputIdx, value);
        }
      } else if (inputVector.noNulls) {
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          checkSizeLimit(length[inputIdx]);
          outputVector.setSafe(outputIdx, vector[inputIdx], start[inputIdx], length[inputIdx]);
        }
      } else {
        final boolean[] isNull = inputVector.isNull;
        for (int i = 0; i < count; i++, inputIdx++, outputIdx++) {
          if (!isNull[inputIdx]) {
            checkSizeLimit(length[inputIdx]);
            outputVector.setSafe(outputIdx, vector[inputIdx], start[inputIdx], length[inputIdx]);
          }
        }
      }
    }
  }
}
