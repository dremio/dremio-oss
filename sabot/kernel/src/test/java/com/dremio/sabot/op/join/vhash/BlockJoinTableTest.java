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
package com.dremio.sabot.op.join.vhash;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.common.ht2.FieldVectorPair;
import com.dremio.sabot.op.common.ht2.HashTable;
import com.dremio.sabot.op.common.ht2.NullComparator;
import com.dremio.sabot.op.common.ht2.PivotBuilder;
import com.dremio.sabot.op.common.ht2.PivotDef;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link BlockJoinTable}
 */
public class BlockJoinTableTest {
  private static final int INITIAL_VAR_FIELD_AVERAGE_SIZE = 10;

  private SabotConfig sabotConfig;
  private OptionManager optionManager;
  private BufferAllocator defaultAllocator;
  private PivotDef probePivot;
  private PivotDef buildPivot;
  private NullComparator comparator;

  @Rule
  public final AllocatorRule defaultAllocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    defaultAllocator = defaultAllocatorRule.newAllocator("test-block_join_table", 0, Long.MAX_VALUE);

    // SabotConfig
    sabotConfig = mockSabotConfig();

    // OptionManager
    optionManager = mockOptionManager();

    // Pivots
    FieldVector intField = new IntVector("intfield", defaultAllocator);

    probePivot = buildPivot(intField);
    buildPivot = buildPivot(intField);

    // Comparator
    BitSet requiredBits = new BitSet();
    comparator = new NullComparator(requiredBits, probePivot.getBitCount());
  }

  @After
  public void cleanupAfterTest() {
    defaultAllocator.close();
  }

  @Test
  public void testInsertingMoreRecordsThanCapacityToNonSpillingHashTable() throws Exception {
    // ARRANGE
    ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.newBuilder().setMinorFragmentId(1).build();
    OperatorContext context = mockOpContext(fragmentHandle, defaultAllocator);

    // Mocks
    HashTable mockedHashTable = mock(HashTable.class);

    when(mockedHashTable.add(anyInt(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).
      thenReturn(3);

    doReturn(mockedHashTable).when(sabotConfig).getInstance(
      anyString(),
      ArgumentMatchers.<Class<HashTable>>any(),
      ArgumentMatchers.<HashTable.HashTableCreateArgs>any()
    );

    // Instantiation
    try (BlockJoinTable table = new BlockJoinTable(buildPivot, probePivot, defaultAllocator, comparator, 0,
      INITIAL_VAR_FIELD_AVERAGE_SIZE, context.getConfig(), context.getOptions(), false)) {

      // ACT
      assertThatThrownBy(() -> table.insert(null, 10))
        .isInstanceOf(OutOfMemoryException.class)
        .hasMessageContaining("Only 3 records");
    }
  }

  private SabotConfig mockSabotConfig() {
    SabotConfig sabotConfig = SabotConfig.create();

    return spy(sabotConfig);
  }

  private OptionManager mockOptionManager() {
    OptionManager optionManager = mock(OptionManager.class);

    when(optionManager.getOption(eq(ExecConstants.RUNTIME_FILTER_VALUE_FILTER_MAX_SIZE))).thenReturn(1000L);
    when(optionManager.getOption(eq(ExecConstants.ENABLE_RUNTIME_FILTER_ON_NON_PARTITIONED_PARQUET))).thenReturn(true);

    return optionManager;
  }

  private OperatorContext mockOpContext(ExecProtos.FragmentHandle fragmentHandle, BufferAllocator allocator) {
    OperatorContext context = mock(OperatorContext.class);
    when(context.getFragmentHandle()).thenReturn(fragmentHandle);
    when(context.getAllocator()).thenReturn(allocator);
    when(context.getOptions()).thenReturn(optionManager);
    when(context.getConfig()).thenReturn(sabotConfig);
    CoordExecRPC.FragmentAssignment assignment = CoordExecRPC.FragmentAssignment.newBuilder().addAllMinorFragmentId(Lists.newArrayList(1, 2, 3, 4)).build();
    when(context.getAssignments()).thenReturn(Lists.newArrayList(assignment));
    return context;
  }

  private PivotDef buildPivot(FieldVector... fields) {
    List<FieldVectorPair> fieldVectors = Arrays.stream(fields).map(
      f -> new FieldVectorPair(f, f)).collect(Collectors.toList());
    return PivotBuilder.getBlockDefinition(fieldVectors);
  }
}
