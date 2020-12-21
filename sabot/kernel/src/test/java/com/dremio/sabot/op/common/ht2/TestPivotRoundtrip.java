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
package com.dremio.sabot.op.common.ht2;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.BaseTestWithAllocator;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

public class TestPivotRoundtrip extends BaseTestWithAllocator {
  private static final int WORD_BITS = 64;

  @Test
  public void intRoundtrip() throws Exception {
    final int count = 1024;
    try (
      IntVector in = new IntVector("in", allocator);
      IntVector out = new IntVector("out", allocator);
    ) {

      in.allocateNew(count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot4Bytes(pivot.getFixedPivots().get(0), fbv, count);

        ValueVector[] ins = new ValueVector[]{in};
        ValueVector[] outs = new ValueVector[]{out};
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, count);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, 100);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 100, 924);
      }
    }
  }

  @Test
  public void intManyRoundtrip() throws Exception {
    final int count = 1024;
    final int mult = 80;

    ValueVector[] in = new IntVector[mult];
    ValueVector[] out = new IntVector[mult];
    List<FieldVectorPair> pairs = new ArrayList<>();
    try {

      for (int x = 0; x < mult; x++) {
        IntVector inv = new IntVector("in", allocator);
        in[x] = inv;
        inv.allocateNew(count);
        IntVector outv = new IntVector("out", allocator);
        out[x] = outv;
        for (int i = 0; i < count; i++) {
          if (i % 5 == 0) {
            inv.setSafe(i, Integer.MAX_VALUE - i);
          }
        }
        inv.setValueCount(count);
        pairs.add(new FieldVectorPair(inv, outv));
      }

      final PivotDef pivot = PivotBuilder.getBlockDefinition(pairs);
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);

        for (int x = 0; x < mult; x++) {
          Pivots.pivot4Bytes(pivot.getFixedPivots().get(x), fbv, count);
        }

        unpivotHelper(pivot, fbv, vbv, in, out, 0, count);
        unpivotHelper(pivot, fbv, vbv, in, out, 0, 100);
        unpivotHelper(pivot, fbv, vbv, in, out, 100, 924);
      }
    } finally {
      AutoCloseables.close(ImmutableList.copyOf(in));
      AutoCloseables.close(ImmutableList.copyOf(out));
    }
  }


  @Test
  public void bigintRoundtrip() throws Exception {
    final int count = 1024;
    try (
      BigIntVector in = new BigIntVector("in", allocator);
      BigIntVector out = new BigIntVector("out", allocator);
    ) {

      in.allocateNew(count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          in.setSafe(i, i);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        ValueVector[] ins = new ValueVector[]{in};
        ValueVector[] outs = new ValueVector[]{out};
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, count);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, 100);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 100, 924);
      }
    }
  }


  @Test
  public void varcharRoundtrip() throws Exception {
    final int count = 1024;
    try (
      VarCharVector in = new VarCharVector("in", allocator);
      VarCharVector out = new VarCharVector("out", allocator);
    ) {

      in.allocateNew(count * 8, count);

      for (int i = 0; i < count; i++) {
        if (i % 5 == 0) {
          byte[] data = ("hello-" + i).getBytes(Charsets.UTF_8);
          in.setSafe(i, data, 0, data.length);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        ValueVector[] ins = new ValueVector[]{in};
        ValueVector[] outs = new ValueVector[]{out};
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, count);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, 100);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 100, 924);
      }
    }
  }

  @Test
  public void decimalRoundtrip() throws Exception {
    final int count = 1024;
    try (
      DecimalVector in = new DecimalVector("in", allocator, 38, 0);
      DecimalVector out = new DecimalVector("out", allocator, 38, 0);
    ) {

      in.allocateNew(count);
      ArrowBuf tempBuf = allocator.buffer(1024);

      // Test data:
      // - in the pivot/unpivot code, each group of 64 values is independently checked whether they're all full, all empty, or some full + some empty
      // - test data will mirror that setup
      assert (count % WORD_BITS) == 0 : String.format("test data code, below, assumes count is a multiple of %d. Instead count=%d", WORD_BITS, count);
      for (int i = 0; i < count; i += WORD_BITS) {
        if ((i / WORD_BITS) % 3 == 0) {
          // all set
          for (int j = 0; j < WORD_BITS; j++) {
            BigDecimal val = BigDecimal.valueOf(i + j + ((double) (i + j) / count)).setScale(0, RoundingMode.HALF_UP);
            DecimalUtility.writeBigDecimalToArrowBuf(val, tempBuf, 0, DecimalVector.TYPE_WIDTH);
            in.set(i + j, tempBuf);
          }
        } else if ((i / WORD_BITS) % 3 == 1) {
          // every 3rd one set: 0, 3, 6, ...
          for (int j = 0; j < WORD_BITS; j++) {
            if (j % 3 == 0) {
              BigDecimal val = BigDecimal.valueOf(i + j + ((double) (i + j) / count)).setScale(0, RoundingMode.HALF_UP);
              DecimalUtility.writeBigDecimalToArrowBuf(val, tempBuf, 0, DecimalVector.TYPE_WIDTH);
              in.set(i + j, tempBuf);
            }
          }
        } else {
          // all blank: no-op
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        ValueVector[] ins = new ValueVector[]{in};
        ValueVector[] outs = new ValueVector[]{out};
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, count);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 0, 100);
        unpivotHelper(pivot, fbv, vbv, ins, outs, 100, 924);
      }

      tempBuf.release();
    }
  }

  @Test
  public void boolRoundtrip() throws Exception {
    final int count = 1024;
    try (
      BitVector in = new BitVector("in", allocator);
      BitVector out = new BitVector("out", allocator);
    ) {

      in.allocateNew(count);
      ArrowBuf tempBuf = allocator.buffer(1024);

      // Test data:
      // - in the pivot/unpivot code, we process in groups of 64 bits at a time
      // - test data will mirror that setup
      assert (count % WORD_BITS) == 0 : String.format("test data code, below, assumes count is a multiple of %d. Instead count=%d", WORD_BITS, count);
      for (int i = 0; i < count; i += WORD_BITS) {
        if ((i / WORD_BITS) % 3 == 0) {
          // all set: F, T, F, T, F, ...
          for (int j = 0; j < WORD_BITS; j++) {
            in.set(i + j, (j & 0x01));
          }
        } else if ((i / WORD_BITS) % 3 == 1) {
          // every 3rd one set: 0, 3, 6, ..., to F, T, F, T, F, ...
          for (int j = 0; j < WORD_BITS; j++) {
            if (j % 3 == 0) {
              in.set(i + j, (j & 0x01));
            }
          }
        } else {
          // all blank: no-op
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        Unpivots.unpivot(pivot, fbv, vbv, 0, count);

        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
      tempBuf.release();
    }
  }

  @Test
  public void boolManyKeys() throws Exception {
    final int count = 1024;
    for (int keyCount = 1; keyCount < 66; keyCount ++) {
      try (
        IntVector intInKey = new IntVector("intInKey", allocator);
        IntVector intOutKey = new IntVector("intOutKey", allocator);
        ) {
        final List<BitVector> inKeys = new ArrayList<>();
        final List<BitVector> outKeys = new ArrayList<>();
        final List<FieldVectorPair> probeFields = new ArrayList<>();

        intInKey.allocateNew(count);

        for (int i = 0; i < count; i++) {
          if (i % 5 == 0) {
            intInKey.setSafe(i, i);
          }
        }
        intOutKey.setValueCount(count);

        probeFields.add(new FieldVectorPair(intInKey, intOutKey));

        BitVector inKey;
        BitVector outKey;
        for (int index = 0; index < keyCount; index++) {
          inKey = new BitVector("inKey_" + index, allocator);
          inKey.allocateNew(count);
          inKeys.add(inKey);

          // Test data:
          // - in the pivot/unpivot code, we process in groups of 64 bits at a time
          // - test data will mirror that setup
          assert (count % WORD_BITS) == 0 : String.format("test data code, below, assumes count is a multiple of %d. Instead count=%d", WORD_BITS, count);
          for (int i = 0; i < count; i += WORD_BITS) {
            if ((i / WORD_BITS) % 3 == 0) {
              // all set: F, T, F, T, F, ...
              for (int j = 0; j < WORD_BITS; j++) {
                inKey.set(i + j, (j & 0x01));
              }
            } else if ((i / WORD_BITS) % 3 == 1) {
              // every 3rd one set: 0, 3, 6, ..., to F, T, F, T, F, ...
              for (int j = 0; j < WORD_BITS; j++) {
                if (j % 3 == 0) {
                  inKey.set(i + j, (j & 0x01));
                }
              }
            } else {
              // all blank: no-op
            }
          }
          inKey.setValueCount(count);

          outKey = new BitVector("outKey_" + index, allocator);
          outKeys.add(outKey);
          probeFields.add(new FieldVectorPair(inKey, outKey));
        }

        final PivotDef pivot = PivotBuilder.getBlockDefinition(probeFields);
        try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
        ) {
          fbv.ensureAvailableBlocks(count);
          Pivots.pivot(pivot, count, fbv, vbv);

          Unpivots.unpivot(pivot, fbv, vbv, 0, count);

          for (int i = 0; i < count; i++) {
            for (int j = 0; j < keyCount; j++) {
              assertEquals(inKeys.get(j).getObject(i), outKeys.get(j).getObject(i));
            }
            assertEquals(intInKey.getObject(i), intOutKey.getObject(i));
          }
        }

        for (int i = 0; i < keyCount; i++) {
          inKeys.get(i).close();
          outKeys.get(i).close();
        }
      }
    }
  }

  @Test
  public void boolNullEveryOther() throws Exception {
    final int count = 1024;
    try (
      BitVector in = new BitVector("in", allocator);
      BitVector out = new BitVector("out", allocator);
    ) {

      in.allocateNew(count);
      ArrowBuf tempBuf = allocator.buffer(1024);

      for (int i = 0; i < count; i ++) {
        if (i % 2 == 0) {
          in.set(i, 1);
        }
      }
      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        Unpivots.unpivot(pivot, fbv, vbv, 0, count);

        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
      tempBuf.release();
    }
  }

  private void unpivotHelper(PivotDef pivot, FixedBlockVector fbv, VariableBlockVector vbv,
                             ValueVector[] in, ValueVector out[], int s, int e) throws Exception {
    Unpivots.unpivot(pivot, fbv, vbv, s, e);
    for (int i = 0; i < (e - s); i++) {
      for (int j = 0; j < in.length; j++) {
        assertEquals(in[j].getObject(s + i), out[j].getObject(i));
      }
    }
    AutoCloseables.close(Arrays.asList(out));
  }

  @Test
  public void pivotWithNulls() throws Exception {
    final int count = 4;
    try (
      BitVector in = new BitVector("in", allocator);
      BitVector out = new BitVector("out", allocator);
    ) {

      in.allocateNew(count);
      ArrowBuf validityBuf = in.getValidityBuffer();
      ArrowBuf valueBuf = in.getValidityBuffer();

      // index 0 : valid and true
      BitVectorHelper.setValidityBit(validityBuf, 0, 1);
      BitVectorHelper.setValidityBit(valueBuf, 0, 1);

      // index 1 : valid and false
      BitVectorHelper.setValidityBit(validityBuf, 1, 1);
      BitVectorHelper.setValidityBit(valueBuf, 1, 0);

      // index 2 : invalid and true
      BitVectorHelper.setValidityBit(validityBuf,2, 0);
      BitVectorHelper.setValidityBit(valueBuf, 2, 1);

      // index 3 : invalid and false
      BitVectorHelper.setValidityBit(validityBuf,3, 0);
      BitVectorHelper.setValidityBit(valueBuf, 3, 0);

      in.setValueCount(count);

      final PivotDef pivot = PivotBuilder.getBlockDefinition(new FieldVectorPair(in, out));
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector vbv = new VariableBlockVector(allocator, pivot.getVariableCount());
      ) {
        fbv.ensureAvailableBlocks(count);
        Pivots.pivot(pivot, count, fbv, vbv);

        Unpivots.unpivot(pivot, fbv, vbv, 0, count);

        for (int i = 0; i < count; i++) {
          assertEquals(in.getObject(i), out.getObject(i));
        }
      }
    }
  }
}
