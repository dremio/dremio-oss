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

import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Random;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.dremio.sabot.BaseTestWithAllocator;

public class TestBoundedPivots extends BaseTestWithAllocator {
  private static final Random RAND = new Random(2342983723452L);

  @Test
  public void fixedOnly(){
    try(IntVector col1 = new IntVector("col1", allocator);
        BigIntVector col2 = new BigIntVector("col2", allocator);
        DecimalVector col3 = new DecimalVector("col3", allocator, 30, 0);
        BitVector col4 = new BitVector("col4", allocator)){

      PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3),
          new FieldVectorPair(col4, col4)
      );
      Integer col1Val[] = populate4ByteValues(col1, 4096);
      Long col2Val[] = populate8ByteValues(col2, 4096);
      BigDecimal col3Val[] = populate16ByteValues(col3, 4096);
      Boolean col4Val[] = populateBooleanValues(col4, 4096);

      assertEquals(32, pivot.getBlockWidth());

      try(FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth(), 4096, true);
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount(), 4096 * 10, true)){
        fixedOnlyHelper(pivot, fixed, variable, 0, 4096, false, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 128, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 17, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 76, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 39, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 189, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 123, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 1023, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 2046, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 3069, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 4092, 4, true, col1Val, col2Val, col3Val, col4Val);
      }
    }
  }

  @Test
  public void fixedOnlyWithoutNull(){
    try(IntVector col1 = new IntVector("col1", allocator);
        BigIntVector col2 = new BigIntVector("col2", allocator);
        DecimalVector col3 = new DecimalVector("col3", allocator, 30, 0);
        BitVector col4 = new BitVector("col4", allocator)){

      PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(col1, col1),
        new FieldVectorPair(col2, col2),
        new FieldVectorPair(col3, col3),
        new FieldVectorPair(col4, col4)
      );
      Integer col1Val[] = populate4ByteValuesWithoutNull(col1, 4096);
      Long col2Val[] = populate8ByteValuesWithoutNull(col2, 4096);
      BigDecimal col3Val[] = populate16ByteValuesWithoutNull(col3, 4096);
      Boolean col4Val[] = populateBooleanValuesWithoutNull(col4, 4096);

      assertEquals(32, pivot.getBlockWidth());

      try(FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth(), 4096, true);
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount(), 4096 * 10, true)){
        fixedOnlyHelper(pivot, fixed, variable, 0, 4096, false, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 128, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 17, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 76, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 39, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 189, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 5, 123, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 0, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 1023, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 2046, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 3069, 1023, true, col1Val, col2Val, col3Val, col4Val);
        fixedOnlyHelper(pivot, fixed, variable, 4092, 4, true, col1Val, col2Val, col3Val, col4Val);
      }
    }
  }

  private void fixedOnlyHelper(PivotDef pivot, FixedBlockVector fixed, VariableBlockVector var, int s, int l,
      boolean reset, Integer[] col1Val, Long[] col2Val, BigDecimal[] col3Val, Boolean[] col4Val) {
    if (reset) {
      fixed.reset();
    }

    BoundedPivots.pivot(pivot, s, l, fixed, var);
    validate4ByteValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(0), fixed, copyOfRange(col1Val, s, s + l));
    validate8ByteValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(1), fixed, copyOfRange(col2Val, s, s + l));
    validate16ByteValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(2), fixed, copyOfRange(col3Val, s, s + l));
    validateBooleanValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(3), fixed, copyOfRange(col4Val, s, s + l));
  }

  @Test
  public void fixedVariable(){
    try(IntVector col1 = new IntVector("col1", allocator);
        BigIntVector col2 = new BigIntVector("col2", allocator);
        VarCharVector col3 = new VarCharVector("col3", allocator);){

      PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
      );
      Integer col1Val[] = populate4ByteValues(col1, 4096);
      Long col2Val[] = populate8ByteValues(col2, 4096);
      String col3Val[] = populateVarCharValues(col3, 4096);

      assertEquals(20, pivot.getBlockWidth());

      try(FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth(), 4096, true);
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount(), 4096 * 2, true)){
        fixedVariableHelper(pivot, fixed, variable, 0, 4096, false, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 128, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 17, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 76, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 39, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 189, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 123, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 1023, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 2046, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 3069, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 4092, 4, true, col1Val, col2Val, col3Val);
      }
    }
  }

  @Test
  public void fixedVariableWithoutNull(){
    try(IntVector col1 = new IntVector("col1", allocator);
        BigIntVector col2 = new BigIntVector("col2", allocator);
        VarCharVector col3 = new VarCharVector("col3", allocator);){

      PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(col1, col1),
        new FieldVectorPair(col2, col2),
        new FieldVectorPair(col3, col3)
      );
      Integer col1Val[] = populate4ByteValuesWithoutNull(col1, 4096);
      Long col2Val[] = populate8ByteValuesWithoutNull(col2, 4096);
      String col3Val[] = populateVarCharValuesWithoutNull(col3, 4096);

      assertEquals(20, pivot.getBlockWidth());

      try(FixedBlockVector fixed = new FixedBlockVector(allocator, pivot.getBlockWidth(), 4096, true);
          VariableBlockVector variable = new VariableBlockVector(allocator, pivot.getVariableCount(), 4096 * 2, true)){
        fixedVariableHelper(pivot, fixed, variable, 0, 4096, false, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 128, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 17, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 76, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 39, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 189, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 5, 123, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 0, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 1023, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 2046, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 3069, 1023, true, col1Val, col2Val, col3Val);
        fixedVariableHelper(pivot, fixed, variable, 4092, 4, true, col1Val, col2Val, col3Val);
      }
    }
  }

  private void fixedVariableHelper(PivotDef pivot, FixedBlockVector fixed, VariableBlockVector var, int s, int l,
      boolean reset, Integer[] col1Val, Long[] col2Val, String[] col3Val) {
    if (reset) {
      fixed.reset();
    }

    final int pivoted = BoundedPivots.pivot(pivot, s, l, fixed, var);
    validate4ByteValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(0), fixed, copyOfRange(col1Val, s, s + pivoted));
    validate8ByteValues(pivot.getBlockWidth(), pivot.getFixedPivots().get(1), fixed, copyOfRange(col2Val, s, s + pivoted));
    validateVarCharValues(pivot.getBlockWidth(), pivot.getVariablePivots().get(0), fixed, var, copyOfRange(col3Val, s, s + pivoted));
  }

  private static void validateBooleanValues(int blockWidth, VectorPivotDef def, FixedBlockVector block, Boolean[] expected){
    int[] expectNulls = new int[expected.length];
    int[] expectValues = new int[expected.length];
    for(int i = 0; i < expected.length; i++){
      Boolean e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e ? 1 : 0;
      }
    }
    int[] actualNulls = new int[expectNulls.length];
    int[] actualValues = new int[expectNulls.length];
    int nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    for(int i = 0; i < expectNulls.length; i++){
      actualNulls[i] =  (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
      actualValues[i] = (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> (nullBitOffsetA + 1)) & 1;
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  private static Boolean[] populateBooleanValues(BitVector vector, int size) {
    assert size >= 4096;
    vector.allocateNew(size);
    Boolean[] booleanValues = new Boolean[size];
    for (int i = 0; i < size; i++) {
      if (i < 64 || (i >= 256 && i < 256 + 64)) {
        vector.setNull(i);
        continue;
      }
      if ((i % 6) != 0) {
        /* every 6th value in boolean column is null */
        if ((i & 1) == 0) {
          /* column value true */
          vector.set(i, 1);
          booleanValues[i] = true;
        } else {
          /* column value false */
          vector.set(i, 0);
          booleanValues[i] = false;
        }
      } else {
        vector.setNull(i);
        booleanValues[i] = null;
      }
    }
    vector.setValueCount(size);
    return booleanValues;
  }

  private static Boolean[] populateBooleanValuesWithoutNull(BitVector vector, int size) {
    assert size >= 4096;
    vector.allocateNew(size);
    Boolean[] booleanValues = new Boolean[size];
    for (int i = 0; i < size; i++) {
      if (i < 64 || (i >= 256 && i < 256 + 64)) {
        vector.setNull(i);
        continue;
      }
      if ((i % 6) != 0) {
        /* every 6th value in boolean column is null */
        if ((i & 1) == 0) {
          /* column value true */
          vector.set(i, 1);
          booleanValues[i] = true;
        } else {
          /* column value false */
          vector.set(i, 0);
          booleanValues[i] = false;
        }
      } else {
        vector.setNull(i);
        booleanValues[i] = null;
      }
    }
    vector.setValueCount(size);
    return booleanValues;
  }

  private static void validate4ByteValues(int blockWidth, VectorPivotDef def, FixedBlockVector block, Integer[] expected){
    int[] expectNulls = new int[expected.length];
    int[] expectValues = new int[expected.length];
    for(int i =0; i < expected.length; i++){
      Integer e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e;
      }
    }
    int[] actualNulls = new int[expectNulls.length];
    int[] actualValues = new int[expectNulls.length];
    int nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    for(int i =0; i < expectNulls.length; i++){
      actualNulls[i] =  (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
      actualValues[i] = buf.getInt((i * blockWidth) + def.getOffset());
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  static Integer[] populate4ByteValues(IntVector vector, int size){
    vector.allocateNew();
    Integer values[] = new Integer[size];
    for(int i =0; i < size; i++){
      if(RAND.nextBoolean()){
        values[i] = RAND.nextInt();
        vector.setSafe(i, values[i]);
      }
    }
    vector.setValueCount(size);
    return values;
  }

  static Integer[] populate4ByteValuesWithoutNull(IntVector vector, int size){
    vector.allocateNew();
    Integer values[] = new Integer[size];
    for(int i =0; i < size; i++){
      values[i] = RAND.nextInt();
      vector.setSafe(i, values[i]);
    }
    vector.setValueCount(size);
    return values;
  }

  private static void validate8ByteValues(int blockWidth, VectorPivotDef def, FixedBlockVector block, Long[] expected){
    long[] expectNulls = new long[expected.length];
    long[] expectValues = new long[expected.length];
    for(int i =0; i < expected.length; i++){
      Long e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e;
      }
    }
    long[] actualNulls = new long[expectNulls.length];
    long[] actualValues = new long[expectNulls.length];
    long nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    for(int i =0; i < expectNulls.length; i++){
      actualNulls[i] =  (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
      actualValues[i] = buf.getLong((i * blockWidth) + def.getOffset());
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  static Long[] populate8ByteValues(BigIntVector vector, int size){
    vector.allocateNew();
    Long values[] = new Long[size];
    for(int i = 0; i < values.length; i++){
      if (RAND.nextBoolean()) {
        values[i] = RAND.nextLong();
        vector.setSafe(i, values[i]);
      }
    }
    vector.setValueCount(values.length);
    return values;
  }

  static Long[] populate8ByteValuesWithoutNull(BigIntVector vector, int size){
    vector.allocateNew();
    Long values[] = new Long[size];
    for(int i = 0; i < values.length; i++){
      values[i] = RAND.nextLong();
      vector.setSafe(i, values[i]);
    }
    vector.setValueCount(values.length);
    return values;
  }

  private void validate16ByteValues(int blockWidth, VectorPivotDef def, FixedBlockVector block, BigDecimal[] expected){
    int[] expectNulls = new int[expected.length];
    BigDecimal[] expectValues = new BigDecimal[expected.length];
    for(int i = 0; i < expected.length; i++){
      BigDecimal e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e;
      }
    }
    int[] actualNulls = new int[expectNulls.length];
    BigDecimal[] actualValues = new BigDecimal[expectNulls.length];
    long nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    try(final ArrowBuf valueBuf = allocator.buffer(16)) {
      for (int i = 0; i < expectNulls.length; i++) {
        actualNulls[i] = (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
        if (actualNulls[i] != 0) {
          buf.getBytes((i * blockWidth) + def.getOffset(), valueBuf, 0, 16);
          actualValues[i] = DecimalUtility.getBigDecimalFromArrowBuf(valueBuf, 0, 0, DecimalVector.TYPE_WIDTH);
        }
      }
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  static BigDecimal[] populate16ByteValues(DecimalVector vector, int size){
    vector.allocateNew();
    BigDecimal values[] = new BigDecimal[size];
    for(int i =0; i < values.length; i++){
      if (RAND.nextBoolean()) {
        values[i] = BigDecimal.valueOf(RAND.nextLong());
        vector.setSafe(i, values[i]);
      }
    }
    vector.setValueCount(values.length);
    return values;
  }

  static BigDecimal[] populate16ByteValuesWithoutNull(DecimalVector vector, int size){
    vector.allocateNew();
    BigDecimal values[] = new BigDecimal[size];
    for(int i =0; i < values.length; i++){
      values[i] = BigDecimal.valueOf(RAND.nextLong());
      vector.setSafe(i, values[i]);
    }
    vector.setValueCount(values.length);
    return values;
  }

  private static void validateVarCharValues(int blockWidth, VectorPivotDef def, FixedBlockVector block,
      VariableBlockVector varBlock, String[] expected){
    int[] expectNulls = new int[expected.length];
    String[] expectValues = new String[expected.length];
    for(int i =0; i < expected.length; i++){
      String e = expected[i];
      if(e != null){
        expectNulls[i] = 1;
        expectValues[i] = e;
      }
    }
    int[] actualNulls = new int[expectNulls.length];
    String[] actualValues = new String[expectNulls.length];
    long nullBitOffsetA = def.getNullBitOffset();
    final ArrowBuf buf = block.getUnderlying();
    for(int i =0; i < expectNulls.length; i++){
      actualNulls[i] =  (buf.getInt(((i * blockWidth) + def.getNullByteOffset())) >>> nullBitOffsetA) & 1;
      if (actualNulls[i] != 0) {
        int offset = buf.getInt(i * blockWidth + blockWidth - 4);
        int len = varBlock.getUnderlying().getInt(offset + 4);
        byte val[] = new byte[len];
        varBlock.getUnderlying().getBytes(offset + 4 + 4, val, 0, len);
        actualValues[i] = new String(val, Charsets.UTF_8);
      }
    }
    assertArrayEquals(expectNulls, actualNulls);
    assertArrayEquals(expectValues, actualValues);
  }

  static String[] populateVarCharValues(VarCharVector vector, int size){
    vector.allocateNew();
    String values[] = new String[size];
    for(int i =0; i < values.length; i++){
      if (RAND.nextBoolean()) {
        values[i] = RandomStringUtils.randomAlphanumeric(RAND.nextInt(25));
        vector.setSafe(i, values[i].getBytes(Charsets.UTF_8));
      }
    }
    vector.setValueCount(values.length);
    return values;
  }

  static String[] populateVarCharValuesWithoutNull(VarCharVector vector, int size){
    vector.allocateNew();
    String values[] = new String[size];
    for(int i =0; i < values.length; i++){
      values[i] = RandomStringUtils.randomAlphanumeric(RAND.nextInt(25));
      vector.setSafe(i, values[i].getBytes(Charsets.UTF_8));
    }
    vector.setValueCount(values.length);
    return values;
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
}
