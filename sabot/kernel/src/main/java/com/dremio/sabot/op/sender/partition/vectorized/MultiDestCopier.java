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
package com.dremio.sabot.op.sender.partition.vectorized;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.CompleteType;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.common.ht2.Reallocators;
import com.dremio.sabot.op.sender.partition.PartitionSenderOperator.Metric;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import io.netty.util.internal.PlatformDependent;

/**
 * Vectorized field buffer copier that handles copying a single field to multiple destinations.<br>
 * Has reference to all destinations' field vectors (2 per destination that point to batches A and B)
 * So from the copier perspective, all batches are different destinations
 */
public abstract class MultiDestCopier {

  private static final int NULL_BUFFER_ORDINAL = 0;
  private static final int VALUE_BUFFER_ORDINAL = 1;
  private static final int OFFSET_BUFFER_ORDINAL = 1;
  private static final int VARIABLE_DATA_BUFFER_ORDINAL = 2;

  private static final int OFFSET_SIZE = 4;

  private final int srcBufferIdx;
  final long[] dstAddrs;

  private final int fieldId;

  private MultiDestCopier(FieldVector[] targets, int fieldId, int bufferOrdinal) {
    this.srcBufferIdx = bufferOrdinal;
    this.fieldId = fieldId;
    dstAddrs = new long[targets.length];
    int i;
    switch (bufferOrdinal) {
      case 0:
        for(i = 0; i < targets.length; i++){
          dstAddrs[i] = targets[i].getValidityBufferAddress();
        }
        break;
      case 1:
        for(i = 0; i < targets.length; i++){
          if (targets[i] instanceof VariableWidthVector) {
            dstAddrs[i] = targets[i].getOffsetBufferAddress();
          } else {
            dstAddrs[i] = targets[i].getDataBufferAddress();
          }
        }
        break;
      case 2:
        for(i = 0; i < targets.length; i++){
          dstAddrs[i] = targets[i].getDataBufferAddress();
        }
        break;
      default:
        throw new UnsupportedOperationException("unexpected buffer offset");
    }
  }

  public int getFieldId() {
    return fieldId;
  }

  /**
   * copy a given number of rows from the incoming buffer to the destinations defined in the offset buffer
   *
   * @param compoundAddr  compound(batchIdx, rowIdx) buffer
   * @param srcStart      row number, in source, copy should start
   * @param count         num rows to copy
   */
  public abstract void copy(long compoundAddr, int srcStart, int count);

  public void updateTargets(int index, FieldVector target) {
    switch (srcBufferIdx) {
      case 0:
        dstAddrs[index] = target.getValidityBufferAddress();
        break;
      case 1:
        if (target instanceof VariableWidthVector) {
          dstAddrs[index] = target.getOffsetBufferAddress();
        } else {
          dstAddrs[index] = target.getDataBufferAddress();
        }
        break;
      case 2:
        dstAddrs[index] = target.getDataBufferAddress();
        break;
      default:
        throw new UnsupportedOperationException("unexpected buffer offset");
    }
  }

  static class FourByteCopier extends MultiDestCopier {
    private static final int SIZE = 4;

    private final FieldVector source;

    private final Stopwatch copyWatch;

    /**
     *
     * @param source field to be copied from the incoming buffer
     * @param targets all target destinations we will be copying to
     */
    FourByteCopier(FieldVector source, int fieldId, FieldVector[] targets, Stopwatch copyWatch) {
      super(targets, fieldId, VALUE_BUFFER_ORDINAL);
      this.source = source;
      this.copyWatch = copyWatch;
    }

    @Override
    public void copy(long compoundAddr, final int srcStart, final int count) {
      copyWatch.start();

      final long[] dstAddrs = this.dstAddrs;

      // compute address of first copied value
      long srcAddr = source.getDataBufferAddress() + srcStart * SIZE;

      final long max = compoundAddr + count * OFFSET_SIZE;
      for (; compoundAddr < max; compoundAddr +=OFFSET_SIZE, srcAddr += SIZE) {
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        PlatformDependent.putInt(dstAddrs[batchIdx] + rowIdx * SIZE, PlatformDependent.getInt(srcAddr));
      }

      copyWatch.stop();
    }
  }

  static class EightByteCopier extends MultiDestCopier {
    private static final int SIZE = 8;

    private final FieldVector source;

    private final Stopwatch copyWatch;

    /**
     *
     * @param source field to be copied from the incoming buffer
     * @param targets all target destinations we will be copying to
     */
    EightByteCopier(FieldVector source, int fieldId, FieldVector[] targets, Stopwatch copyWatch) {
      super(targets, fieldId, VALUE_BUFFER_ORDINAL);
      this.source = source;
      this.copyWatch = copyWatch;
    }

    @Override
    public void copy(long compoundAddr, final int srcStart, final int count) {
      copyWatch.start();

      final long[] dstAddrs = this.dstAddrs;

      long srcAddr = source.getDataBufferAddress() + srcStart * SIZE;

      final long max = compoundAddr + count * OFFSET_SIZE;
      for (; compoundAddr < max; compoundAddr +=OFFSET_SIZE, srcAddr += SIZE) {
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        PlatformDependent.putLong(dstAddrs[batchIdx] + rowIdx * SIZE, PlatformDependent.getLong(srcAddr));
      }

      copyWatch.stop();
    }
  }

  static class SixteenByteCopier extends MultiDestCopier {
    private static final int SIZE = 16;

    private final FieldVector source;

    private final Stopwatch copyWatch;

    /**
     * @param source field to be copied from the incoming buffer
     * @param targets all target destinations we will be copying to
     */
    SixteenByteCopier(FieldVector source, int fieldId, FieldVector[] targets, Stopwatch copyWatch) {
      super(targets, fieldId, VALUE_BUFFER_ORDINAL);
      this.source = source;
      this.copyWatch = copyWatch;
    }

    @Override
    public void copy(long compoundAddr, final int srcStart, final int count) {
      copyWatch.start();

      final long[] dstAddrs = this.dstAddrs;

      long srcAddr = source.getDataBufferAddress() + srcStart * SIZE;

      final long max = compoundAddr + count * OFFSET_SIZE;
      for (; compoundAddr < max; compoundAddr +=OFFSET_SIZE, srcAddr += SIZE) {
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        final long dstAddr = dstAddrs[batchIdx] + rowIdx * SIZE;
        PlatformDependent.putLong(dstAddr, PlatformDependent.getLong(srcAddr));
        PlatformDependent.putLong(dstAddr + 8, PlatformDependent.getLong(srcAddr + 8));
      }

      copyWatch.stop();
    }
  }

  static class VariableCopier extends MultiDestCopier {
    private final FieldVector source;
    private final Reallocators.Reallocator[] reallocs;

    private final long[] dstOffsetAddrs;

    private final Stopwatch copyWatch;

    VariableCopier(FieldVector source, int fieldId, FieldVector[] targets, Stopwatch copyWatch) {
      super(targets, fieldId, VARIABLE_DATA_BUFFER_ORDINAL);
      this.source = source;
      final int numTargets = targets.length;
      this.reallocs = new Reallocators.Reallocator[numTargets];
      dstOffsetAddrs = new long[numTargets];
      for (int i = 0; i < numTargets; i++) {
        reallocs[i] = Reallocators.getReallocator(targets[i]);
        dstOffsetAddrs[i] = targets[i].getOffsetBufferAddress();
      }

      this.copyWatch = copyWatch;
    }

    @Override
    public void updateTargets(int index, FieldVector target) {
      super.updateTargets(index, target);
      dstOffsetAddrs[index] = target.getOffsetBufferAddress();
    }

    @Override
    public void copy(long compoundAddr, int srcStart, int count) {
      copyWatch.start();

      // source data/offset buffers
      final long srcDataAddr = source.getDataBufferAddress();
      long srcOffsetAddr = source.getOffsetBufferAddress();

      // go the first copied record from src. recordIdx = srcSkip
      srcOffsetAddr += srcStart * 4;
      int srcOffset = PlatformDependent.getInt(srcOffsetAddr);
      srcOffsetAddr += 4; // srcOffsetAddr always points to the next offset

      final long max = compoundAddr + count * OFFSET_SIZE;
      for (; compoundAddr < max; compoundAddr +=OFFSET_SIZE) {
        // compute the length of the value we are about to copy from src
        final int nextSrcOffset = PlatformDependent.getInt(srcOffsetAddr);
        final int len = nextSrcOffset - srcOffset;

        // figure out where we need to copy
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        // compute the offset of the destination
        final long dstOffsetAddr = dstOffsetAddrs[batchIdx] + rowIdx * 4;
        final int dstOffset = PlatformDependent.getInt(dstOffsetAddr);
        long dstDataAddr = dstAddrs[batchIdx] + dstOffset;

        // ensure target buffer is big enough
        final Reallocators.Reallocator realloc = reallocs[batchIdx];
        if(dstDataAddr + len > realloc.max()){
          final long newDataAddr = realloc.ensure(dstOffset + len);
          dstDataAddr = newDataAddr + dstOffset;
          dstAddrs[batchIdx] = newDataAddr;
        }

        // copy the value from src to dst
        com.dremio.sabot.op.common.ht2.Copier.copy(srcDataAddr + srcOffset, dstDataAddr, len);
        PlatformDependent.putInt(dstOffsetAddr + 4, dstOffset + len);
        // update dst offset

        // move to the next src value
        srcOffsetAddr += 4;
        srcOffset = nextSrcOffset;
      }

      copyWatch.stop();
    }
  }

  static class BitCopier extends MultiDestCopier {

    private final FieldVector source;
    private final int bufferOrdinal;

    private final Stopwatch copyWatch;

    BitCopier(FieldVector source, int fieldId, FieldVector[] targets, int bufferOrdinal, Stopwatch copyWatch) {
      super(targets, fieldId, bufferOrdinal);
      this.source = source;
      this.bufferOrdinal = bufferOrdinal;
      this.copyWatch = copyWatch;
    }

    @Override
    public void copy(long compoundAddr, int srcStart, final int count) {
      copyWatch.start();

      final long[] dstAddrs = this.dstAddrs;

      // skip bytes, but make sure to account for the remaining bits too
      final long srcAddr;
      switch (bufferOrdinal) {
        case NULL_BUFFER_ORDINAL:
          srcAddr = source.getValidityBufferAddress();
          break;
        case VALUE_BUFFER_ORDINAL:
          srcAddr = source.getDataBufferAddress();
          break;
        default:
          throw new UnsupportedOperationException("unexpected buffer offset");
      }

      final long max = compoundAddr + count * OFFSET_SIZE;
      for(; compoundAddr < max; compoundAddr +=OFFSET_SIZE, srcStart++){
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        final int byteValue = PlatformDependent.getByte(srcAddr + (srcStart >>> 3));
        final int bitVal = ((byteValue >>> (srcStart & 7)) & 1) << (rowIdx & 7);
        final long dstAddr = dstAddrs[batchIdx] + (rowIdx >>> 3);
        PlatformDependent.putByte(dstAddr, (byte) (PlatformDependent.getByte(dstAddr) | bitVal));
      }

      copyWatch.stop();
    }

  }

  static class GenericCopier extends MultiDestCopier {
    private final TransferPair[] transfers;

    private final Stopwatch copyWatch;

    GenericCopier(FieldVector source, int fieldId, FieldVector[] targets, Stopwatch copyWatch) {
      super(targets, fieldId, 0); // bufferOrdinal doesn't matter here
      final int numTargets = targets.length;
      this.transfers = new TransferPair[numTargets];

      for (int i = 0; i < numTargets; i++) {
        transfers[i] = source.makeTransferPair(targets[i]);
      }

      this.copyWatch = copyWatch;
    }

    @Override
    public void copy(long compoundAddr, final int srcStart, int count) {
      copyWatch.start();

      final long max = compoundAddr + count * OFFSET_SIZE;
      for(int from = srcStart; compoundAddr < max; compoundAddr +=OFFSET_SIZE, from++) {
        final int compoundIdx = PlatformDependent.getInt(compoundAddr);
        final int batchIdx = compoundIdx >>> 16;
        final int rowIdx = compoundIdx & 65535;

        transfers[batchIdx].copyValueSafe(from, rowIdx);
      }

      copyWatch.stop();
    }
  }

  private static boolean sameClass(FieldVector source, FieldVector[] targets) {
    for (FieldVector target : targets) {
      if (source.getClass() != target.getClass()) {
        return false;
      }
    }
    return true;
  }

  private static void addValueCopier(final FieldVector source, final int fieldId, final FieldVector[] targets,
                                     ImmutableList.Builder<MultiDestCopier> copiers, CopyWatches copyWatches) {
    Preconditions.checkArgument(sameClass(source, targets), "Input and output vectors must be same type.");
    switch (CompleteType.fromField(source.getField()).toMinorType()) {

      case TIMESTAMP:
      case FLOAT8:
      case BIGINT:
      case INTERVALDAY:
      case DATE:
        copiers.add(new EightByteCopier(source, fieldId, targets, copyWatches.getFixed()));
        copiers.add(new BitCopier(source, fieldId, targets, NULL_BUFFER_ORDINAL, copyWatches.getBinary()));
        break;
      case BIT:
        copiers.add(new BitCopier(source, fieldId, targets, VALUE_BUFFER_ORDINAL, copyWatches.getBinary()));
        copiers.add(new BitCopier(source, fieldId, targets, NULL_BUFFER_ORDINAL, copyWatches.getBinary()));
        break;

      case TIME:
      case FLOAT4:
      case INT:
      case INTERVALYEAR:
        copiers.add(new FourByteCopier(source, fieldId, targets, copyWatches.getFixed()));
        copiers.add(new BitCopier(source, fieldId, targets, NULL_BUFFER_ORDINAL, copyWatches.getBinary()));
        break;

      case VARBINARY:
      case VARCHAR:
        copiers.add(new VariableCopier(source, fieldId, targets, copyWatches.getVariable()));
        copiers.add(new BitCopier(source, fieldId, targets, NULL_BUFFER_ORDINAL, copyWatches.getBinary()));
        break;

      case DECIMAL:
        copiers.add(new SixteenByteCopier(source, fieldId, targets, copyWatches.getFixed()));
        copiers.add(new BitCopier(source, fieldId, targets, NULL_BUFFER_ORDINAL, copyWatches.getBinary()));
        break;

      case MAP:
      case LIST:
      case STRUCT:
      case UNION:
        copiers.add(new GenericCopier(source, fieldId, targets, copyWatches.getGeneric()));
        break;

      default:
        throw new UnsupportedOperationException("Unknown type to copy.");
    }
  }

  static ImmutableList<MultiDestCopier> getCopiers(final List<FieldVector> inputs, OutgoingBatch[] batches,
                                                          CopyWatches copyWatches) {
    ImmutableList.Builder<MultiDestCopier> copiers = ImmutableList.builder();
    final int numFields = inputs.size();
    final int numBatches = batches.length;

    // for each field group all corresponding field vectors for all batches together
    // outputs[f][b] = field vector f for batch b
    final FieldVector[][] outputs = new FieldVector[numFields][numBatches];
    for (int b = 0; b < numBatches; b++) {
      final List<FieldVector> fieldVectors = batches[b].getFieldVectors();
      Preconditions.checkArgument(numFields == fieldVectors.size(), "Input and output lists must be same size.");
      for (int f = 0; f < numFields; f++) {
        outputs[f][b] = fieldVectors.get(f);
      }
    }

    for (int i = 0; i < inputs.size(); i++) {
      final FieldVector input = inputs.get(i);
      final FieldVector[] targets = outputs[i];
      addValueCopier(input, i, targets, copiers, copyWatches);
    }
    return copiers.build();
  }

  public static class CopyWatches {
    private final Stopwatch fixedCopy = Stopwatch.createUnstarted();
    private final Stopwatch variableCopy = Stopwatch.createUnstarted();
    private final Stopwatch binaryCopy = Stopwatch.createUnstarted();
    private final Stopwatch genericCopy = Stopwatch.createUnstarted();

    void updateStats(OperatorStats stats) {
      stats.setLongStat(Metric.FIXED_COPY_NS, fixedCopy.elapsed(NANOSECONDS));
      stats.setLongStat(Metric.VARIABLE_COPY_NS, variableCopy.elapsed(NANOSECONDS));
      stats.setLongStat(Metric.BINARY_COPY_NS, binaryCopy.elapsed(NANOSECONDS));
      stats.setLongStat(Metric.GENERIC_COPY_NS, genericCopy.elapsed(NANOSECONDS));
    }

    public Stopwatch getFixed() {
      return fixedCopy;
    }

    public Stopwatch getVariable() {
      return variableCopy;
    }

    public Stopwatch getBinary() {
      return binaryCopy;
    }

    public Stopwatch getGeneric() {
      return genericCopy;
    }
  }

}
