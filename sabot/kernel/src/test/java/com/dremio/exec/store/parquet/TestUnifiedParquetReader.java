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
package com.dremio.exec.store.parquet;

import static com.google.common.base.Predicates.alwaysTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CompositeColumnFilter;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link UnifiedParquetReader}
 */
public class TestUnifiedParquetReader {
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-unifiedparquetreader", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }
    @Test
    public void testDropIncompatibleRuntimeFiltersSingleRSingleC() {
        try(IntVector vv = new IntVector("col1", testAllocator)) {
            OperatorStats stats = mock(OperatorStats.class);
            doNothing().when(stats).addLongStat(any(MetricDef.class), anyLong());
            OperatorContext ctx = mock(OperatorContext.class);
            when(ctx.getStats()).thenReturn(stats);

            OutputMutator output = mock(OutputMutator.class);
            when(output.getVector(anyString())).thenReturn(vv);

            Map<String, Types.MinorType> cols = new HashMap<>(1);
            cols.put("col1", Types.MinorType.BIGINT);
            RuntimeFilter filter = prepareTestFilter(false, cols);

            UnifiedParquetReader instance = getInstance(ctx);
            instance.addRuntimeFilter(filter);
            instance.dropIncompatibleRuntimeFilters(output);

            assertTrue(instance.getRuntimeFilters().isEmpty());
            verify(stats).addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);
        }
    }

    @Test
    public void testDropIncompatibleRuntimeFiltersSingleRMultiC() {
        try(IntVector vv1 = new IntVector("col1", testAllocator);
            VarCharVector vv2 = new VarCharVector("col2", testAllocator)) {
            OperatorStats stats = mock(OperatorStats.class);
            doNothing().when(stats).addLongStat(any(MetricDef.class), anyLong());
            OperatorContext ctx = mock(OperatorContext.class);
            when(ctx.getStats()).thenReturn(stats);

            OutputMutator output = mock(OutputMutator.class);
            when(output.getVector(eq("col1"))).thenReturn(vv1);
            when(output.getVector(eq("col2"))).thenReturn(vv2);

            Map<String, Types.MinorType> cols = new HashMap<>(1);
            cols.put("col1", Types.MinorType.BIGINT);
            cols.put("col2", Types.MinorType.VARBINARY);
            RuntimeFilter filter = prepareTestFilter(false, cols);

            UnifiedParquetReader instance = getInstance(ctx);
            instance.addRuntimeFilter(filter);
            instance.dropIncompatibleRuntimeFilters(output);

            assertTrue(instance.getRuntimeFilters().isEmpty());
            verify(stats, times(2)).addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);

            cols.put("col2", Types.MinorType.VARCHAR);
            RuntimeFilter filter2 = prepareTestFilter(false, cols);
            instance.addRuntimeFilter(filter2);
            instance.dropIncompatibleRuntimeFilters(output);
            assertFalse(instance.getRuntimeFilters().isEmpty());
            assertEquals(1, instance.getRuntimeFilters().get(0).getNonPartitionColumnFilters().size());
            assertEquals("col2", instance.getRuntimeFilters().get(0).getNonPartitionColumnFilters().get(0).getColumnsList().get(0));
            verify(stats, times(3)).addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);
        }
    }

    @Test
    public void testDropIncompatibleRuntimeFiltersMultiRMultiC() {
        try(IntVector vv1 = new IntVector("col1", testAllocator);
            VarCharVector vv2 = new VarCharVector("col2", testAllocator)) {
            OperatorStats stats = mock(OperatorStats.class);
            doNothing().when(stats).addLongStat(any(MetricDef.class), anyLong());
            OperatorContext ctx = mock(OperatorContext.class);
            when(ctx.getStats()).thenReturn(stats);

            OutputMutator output = mock(OutputMutator.class);
            when(output.getVector(eq("col1"))).thenReturn(vv1);
            when(output.getVector(eq("col2"))).thenReturn(vv2);

            Map<String, Types.MinorType> cols = new HashMap<>(1);
            cols.put("col1", Types.MinorType.BIGINT);
            cols.put("col2", Types.MinorType.VARBINARY);
            RuntimeFilter filter1 = prepareTestFilter(false, cols);
            RuntimeFilter filter2 = prepareTestFilter(false, cols);

            UnifiedParquetReader instance = getInstance(ctx);
            instance.addRuntimeFilter(filter1);
            instance.addRuntimeFilter(filter2);
            instance.dropIncompatibleRuntimeFilters(output);

            assertTrue(instance.getRuntimeFilters().isEmpty());
            verify(stats, times(4)).addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);

            cols.put("col2", Types.MinorType.VARCHAR);
            RuntimeFilter filter3 = prepareTestFilter(false, cols);
            RuntimeFilter filter4 = prepareTestFilter(false, cols);
            instance.addRuntimeFilter(filter3);
            instance.addRuntimeFilter(filter4);
            instance.dropIncompatibleRuntimeFilters(output);
            assertEquals(2, instance.getRuntimeFilters().size());
            assertEquals(1, instance.getRuntimeFilters().get(0).getNonPartitionColumnFilters().size());
            assertEquals(1, instance.getRuntimeFilters().get(1).getNonPartitionColumnFilters().size());
            assertEquals("col2", instance.getRuntimeFilters().get(0).getNonPartitionColumnFilters().get(0).getColumnsList().get(0));
            assertEquals("col2", instance.getRuntimeFilters().get(1).getNonPartitionColumnFilters().get(0).getColumnsList().get(0));
            verify(stats, times(6)).addLongStat(ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT, 1);

            instance.getRuntimeFilters().removeIf(alwaysTrue());
            instance.addRuntimeFilter(filter2);
            instance.addRuntimeFilter(filter3);
            instance.dropIncompatibleRuntimeFilters(output);
            assertEquals(1, instance.getRuntimeFilters().size());
        }
    }

    private RuntimeFilter prepareTestFilter(boolean partitionColPresent, Map<String, Types.MinorType> cols) {
        final List<CompositeColumnFilter> colFilters = new ArrayList<>(cols.size());
        for (Map.Entry<String, Types.MinorType> colEntry : cols.entrySet()) {
            ValueListFilter vlf = mock(ValueListFilter.class);
            when(vlf.getFieldName()).thenReturn(colEntry.getKey());
            when(vlf.getFieldType()).thenReturn(colEntry.getValue());
            CompositeColumnFilter colFilter = new CompositeColumnFilter.Builder()
                    .setFilterType(CompositeColumnFilter.RuntimeFilterType.VALUE_LIST)
                    .setColumnsList(Lists.newArrayList(colEntry.getKey()))
                    .setValueList(vlf)
                    .build();
            colFilters.add(colFilter);
        }
        CompositeColumnFilter partitionColFilter = null;

        if (partitionColPresent) {
            partitionColFilter = new CompositeColumnFilter.Builder()
                    .setFilterType(CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER)
                    .setColumnsList(Lists.newArrayList("pc1"))
                    .setBloomFilter(mock(BloomFilter.class))
                    .build();
        }
        return new RuntimeFilter(partitionColFilter, colFilters, "");
    }

    @Test
    public void testDropIncompatibleRuntimeFiltersEmpty() {
        OperatorContext ctx = mock(OperatorContext.class);
        OutputMutator output = mock(OutputMutator.class);

        UnifiedParquetReader instance = getInstance(ctx);
        instance.dropIncompatibleRuntimeFilters(output);
        // No exceptions expected
        assertTrue(instance.getRuntimeFilters().isEmpty());
    }

    private UnifiedParquetReader getInstance(OperatorContext ctx) {
        return new UnifiedParquetReader(
                ctx,
                mock(ParquetReaderFactory.class),
                mock(BatchSchema.class),
                mock(ParquetScanProjectedColumns.class),
                new HashMap<>(),
                new ArrayList<>(),
                mock(ParquetFilterCreator.class),
                mock(ParquetDictionaryConvertor.class),
                null,
                mock(FileSystem.class),
                mock(MutableParquetMetadata.class),
                mock(GlobalDictionaries.class),
                mock(SchemaDerivationHelper.class),
                true,
                true,
                true,
                mock(InputStreamProvider.class),
                new ArrayList<>()
                );
    }
}
