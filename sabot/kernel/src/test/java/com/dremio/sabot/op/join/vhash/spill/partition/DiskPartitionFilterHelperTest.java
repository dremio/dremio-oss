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

package com.dremio.sabot.op.join.vhash.spill.partition;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashTableKeyReader;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.test.AllocatorRule;
import io.netty.util.internal.PlatformDependent;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class DiskPartitionFilterHelperTest {
  private RuntimeFilterProbeTarget probeTarget;
  private BloomFilter bloomFilter;
  private HashTableKeyReader keyReader;
  private ArrowBuf keyHolder;
  private ArrowBuf key;
  private ValueListFilterBuilder valueListFilterBuilder;
  private int records = 10;
  private int batchSize = 10;
  private int blockWidth = 4;
  private BufferAllocator testAllocator;
  private PivotDef pivot;
  private List<HashTableKeyReader> keyReaderList;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator =
        allocatorRule.newAllocator("test-DiskPartitionFilterHelperTest", 0, Long.MAX_VALUE);
    probeTarget = Mockito.mock(RuntimeFilterProbeTarget.class);
    bloomFilter = Mockito.mock(BloomFilter.class);
    keyReader = Mockito.mock(HashTableKeyReader.class);
    keyHolder = Mockito.mock(ArrowBuf.class);
    key = Mockito.mock(ArrowBuf.class);
    valueListFilterBuilder = Mockito.mock(ValueListFilterBuilder.class);
    keyReaderList = Arrays.asList(keyReader);
    pivot = Mockito.mock(PivotDef.class);
  }

  @After
  public void cleanupAfterTest() {
    testAllocator.close();
  }

  @Test
  public void testPrepareBloomFilters() {
    try (ArrowBuf sv2 = getFilledSV2(testAllocator, batchSize);
        final FixedBlockVector fixed =
            getFilledFixedBlockVector(testAllocator, blockWidth, batchSize);
        final VariableBlockVector var = new VariableBlockVector(testAllocator, 2); ) {
      when(probeTarget.getPartitionBuildTableKeys()).thenReturn(Arrays.asList("test1", "test2"));
      when(keyReader.getKeyHolder()).thenReturn(keyHolder);
      when(keyReader.getKeyBufSize()).thenReturn(10);
      when(bloomFilter.isCrossingMaxFPP()).thenReturn(false);
      DiskPartitionFilterHelper.prepareBloomFilters(
          probeTarget, Optional.of(bloomFilter), keyReader, fixed, var, 0, records, sv2);
      verify(bloomFilter, times(10)).put(keyHolder, 10);
      verify(keyReader, times(10)).loadNextKey(any(Long.class), any(Long.class));
      verify(probeTarget).getPartitionBuildTableKeys();
      verify(keyReader).getKeyHolder();
      verify(bloomFilter).isCrossingMaxFPP();
    }
  }

  @Test
  public void testPrepareValueListFilters() {
    try (ArrowBuf sv2 = getFilledSV2(testAllocator, batchSize);
        final FixedBlockVector fixed =
            getFilledFixedBlockVector(testAllocator, blockWidth, batchSize);
        final VariableBlockVector var = new VariableBlockVector(testAllocator, 2); ) {
      when(probeTarget.getPartitionBuildTableKeys()).thenReturn(Arrays.asList("test1", "test2"));
      when(keyReader.getKeyValBuf()).thenReturn(key);
      when(keyReader.getKeyBufSize()).thenReturn(10);
      when(keyReader.areAllValuesNull()).thenReturn(false);
      when(valueListFilterBuilder.getFieldName()).thenReturn("test1");
      when(pivot.isBoolField(any(String.class))).thenReturn(false);
      when(valueListFilterBuilder.insert(key)).thenReturn(true);
      DiskPartitionFilterHelper.prepareValueListFilters(
          keyReaderList, Arrays.asList(valueListFilterBuilder), pivot, fixed, var, 0, records, sv2);
      verify(valueListFilterBuilder, times(10)).insert(key);
      verify(keyReader, times(10)).loadNextKey(any(Long.class), any(Long.class));
    }
  }

  @Test
  public void testPrepareValueListFiltersWithBooleanField() {
    try (ArrowBuf sv2 = getFilledSV2(testAllocator, batchSize);
        final FixedBlockVector fixed =
            getFilledFixedBlockVector(testAllocator, blockWidth, batchSize);
        final VariableBlockVector var = new VariableBlockVector(testAllocator, 2); ) {
      when(probeTarget.getPartitionBuildTableKeys()).thenReturn(Arrays.asList("test1", "test2"));
      when(keyReader.getKeyHolder()).thenReturn(keyHolder);
      when(keyReader.getKeyValBuf()).thenReturn(key);
      when(keyReader.getKeyBufSize()).thenReturn(10);
      when(keyReader.areAllValuesNull()).thenReturn(false);
      when(valueListFilterBuilder.getFieldName()).thenReturn("test1");
      when(pivot.isBoolField(any(String.class))).thenReturn(true);
      DiskPartitionFilterHelper.prepareValueListFilters(
          keyReaderList, Arrays.asList(valueListFilterBuilder), pivot, fixed, var, 0, records, sv2);
      verify(valueListFilterBuilder, times(10)).insertBooleanVal(any(Boolean.class));
      verify(keyReader, times(10)).loadNextKey(any(Long.class), any(Long.class));
      verify(keyReader, times(10)).getKeyHolder();
      verify(keyReader, times(1)).getKeyValBuf();
    }
  }

  @Test
  public void testPrepareValueListFiltersWithNullFiled() {
    try (ArrowBuf sv2 = getFilledSV2(testAllocator, batchSize);
        final FixedBlockVector fixed =
            getFilledFixedBlockVector(testAllocator, blockWidth, batchSize);
        final VariableBlockVector var = new VariableBlockVector(testAllocator, 2); ) {
      when(probeTarget.getPartitionBuildTableKeys()).thenReturn(Arrays.asList("test1", "test2"));
      when(keyReader.getKeyValBuf()).thenReturn(key);
      when(keyReader.getKeyBufSize()).thenReturn(10);
      when(keyReader.areAllValuesNull()).thenReturn(true);
      when(valueListFilterBuilder.getFieldName()).thenReturn("test1");
      when(pivot.isBoolField(any(String.class))).thenReturn(false);
      DiskPartitionFilterHelper.prepareValueListFilters(
          keyReaderList, Arrays.asList(valueListFilterBuilder), pivot, fixed, var, 0, records, sv2);
      verify(valueListFilterBuilder, times(10)).insertNull();
      verify(keyReader, times(10)).loadNextKey(any(Long.class), any(Long.class));
      verify(keyReader, times(1)).getKeyValBuf();
    }
  }

  @Test
  public void testPrepareValueListFiltersWithVarNull() {
    try (ArrowBuf sv2 = getFilledSV2(testAllocator, batchSize);
        final FixedBlockVector fixed =
            getFilledFixedBlockVector(testAllocator, blockWidth, batchSize);
        final VariableBlockVector var = null) {
      when(probeTarget.getPartitionBuildTableKeys()).thenReturn(Arrays.asList("test1", "test2"));
      when(keyReader.getKeyValBuf()).thenReturn(key);
      when(keyReader.getKeyBufSize()).thenReturn(10);
      when(keyReader.areAllValuesNull()).thenReturn(true);
      when(valueListFilterBuilder.getFieldName()).thenReturn("test1");
      when(pivot.isBoolField(any(String.class))).thenReturn(false);
      DiskPartitionFilterHelper.prepareValueListFilters(
          keyReaderList, Arrays.asList(valueListFilterBuilder), pivot, fixed, var, 0, records, sv2);
      verify(valueListFilterBuilder, times(10)).insertNull();
      verify(keyReader, times(10)).loadNextKey(any(Long.class), any(Long.class));
      verify(keyReader, times(1)).getKeyValBuf();
    }
  }

  private ArrowBuf getFilledSV2(BufferAllocator allocator, int batchSize) {
    ArrowBuf buf = allocator.buffer((long) SelectionVector2.RECORD_SIZE * batchSize);
    long addr = buf.memoryAddress();
    for (int i = 0; i < batchSize; ++i) {
      PlatformDependent.putShort(addr, (short) i);
      addr += SelectionVector2.RECORD_SIZE;
    }
    return buf;
  }

  private FixedBlockVector getFilledFixedBlockVector(
      BufferAllocator allocator, int blockWidth, int batchSize) {
    final FixedBlockVector fixed = new FixedBlockVector(allocator, blockWidth, 32, true);
    ArrowBuf buf = fixed.getBuf();
    long addr = buf.memoryAddress();
    for (int i = 0; i < batchSize; ++i) {
      PlatformDependent.putInt(addr, i);
      addr += SelectionVector2.RECORD_SIZE;
    }
    return fixed;
  }
}
