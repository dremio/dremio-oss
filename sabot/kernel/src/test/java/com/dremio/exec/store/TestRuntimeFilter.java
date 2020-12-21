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

package com.dremio.exec.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.Lists;

/**
 * Tests for {@link RuntimeFilter}
 */
public class TestRuntimeFilter {
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-runtimefilter", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }

    @Test
    public void testIsOnSameColumnNonPartitionColumnsNoMatch() throws Exception {
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {

            CompositeColumnFilter ccf1 = newValListFilterLong("long_field", Lists.newArrayList(1L, 200L, 1500L, 4900L));
            CompositeColumnFilter ccf2 = newValListFilterInt("int_field", Lists.newArrayList(1, 900, 1501, 4900));
            CompositeColumnFilter ccf3 = newValListFilterInt("int_field", Lists.newArrayList(50, 890, 1550, 4990));
            RuntimeFilter filter1 = new RuntimeFilter(null, Lists.newArrayList(ccf1, ccf2), null);
            closer.add(filter1);
            RuntimeFilter filter2 = new RuntimeFilter(null, Lists.newArrayList(ccf3), null);
            closer.add(filter2);

            assertFalse(filter1.isOnSameColumns(filter2));
            assertFalse(filter2.isOnSameColumns(filter1));
        }
    }

    @Test
    public void testIsOnSameColumnNonPartitionColumnsMatch() throws Exception {
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {

            CompositeColumnFilter ccf1 = newValListFilterLong("long_field", Lists.newArrayList(1L, 200L, 1500L, 4900L));
            CompositeColumnFilter ccf2 = newValListFilterInt("int_field", Lists.newArrayList(1, 900, 1501, 4900));
            CompositeColumnFilter ccf3 = newValListFilterInt("int_field", Lists.newArrayList(50, 890, 1550, 4990));
            CompositeColumnFilter ccf4 = newValListFilterLong("long_field", Lists.newArrayList(1L, 200L, 1500L, 4900L));

            RuntimeFilter filter1 = new RuntimeFilter(null, Lists.newArrayList(ccf1, ccf2), null);
            closer.add(filter1);
            RuntimeFilter filter2 = new RuntimeFilter(null, Lists.newArrayList(ccf3, ccf4), null);
            closer.add(filter2);

            assertTrue(filter1.isOnSameColumns(filter2));
            assertTrue(filter2.isOnSameColumns(filter1));
        }
    }

    @Test
    public void testIsOnSameColumnPartitionColumnsMatch() throws Exception {
        CompositeColumnFilter ccf1 = newBloomFilter(Lists.newArrayList("col1", "col2"));
        CompositeColumnFilter ccf2 = newBloomFilter(Lists.newArrayList("col2", "col1"));
        CompositeColumnFilter ccf3 = newBloomFilter(Lists.newArrayList("col1", "col2", "col3"));
        CompositeColumnFilter ccf4 = newBloomFilter(Lists.newArrayList("col3"));
        RuntimeFilter filter1 = new RuntimeFilter(ccf1, Collections.EMPTY_LIST, null);
        RuntimeFilter filter2 = new RuntimeFilter(ccf2, Collections.EMPTY_LIST, null);
        RuntimeFilter filter3 = new RuntimeFilter(null, Collections.EMPTY_LIST, null);
        RuntimeFilter filter4 = new RuntimeFilter(ccf3, Collections.EMPTY_LIST, null);
        RuntimeFilter filter5 = new RuntimeFilter(ccf4, Collections.EMPTY_LIST, null);

        assertTrue(filter1.isOnSameColumns(filter2));
        assertTrue(filter2.isOnSameColumns(filter1));

        assertFalse(filter1.isOnSameColumns(filter3));
        assertFalse(filter3.isOnSameColumns(filter2));

        assertFalse(filter1.isOnSameColumns(filter4));
        assertFalse(filter4.isOnSameColumns(filter1));

        assertFalse(filter1.isOnSameColumns(filter5));
        assertFalse(filter5.isOnSameColumns(filter1));

        assertFalse(filter4.isOnSameColumns(filter5));
        assertFalse(filter5.isOnSameColumns(filter4));
    }

    @Test
    public void testIsOnSameColumn() throws Exception {
        try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {

            CompositeColumnFilter pccf1 = newBloomFilter(Lists.newArrayList("col1", "col2"));
            CompositeColumnFilter pccf2 = newBloomFilter(Lists.newArrayList("col2", "col1"));
            CompositeColumnFilter pccf3 = newBloomFilter(Lists.newArrayList("col1", "col2", "col3"));
            CompositeColumnFilter pccf4 = newBloomFilter(Lists.newArrayList("col3"));

            CompositeColumnFilter ccf1 = newValListFilterLong("long_field", Lists.newArrayList(1L, 200L, 1500L, 4900L));
            CompositeColumnFilter ccf2 = newValListFilterInt("int_field", Lists.newArrayList(1, 900, 1501, 4900));
            CompositeColumnFilter ccf3 = newValListFilterInt("int_field", Lists.newArrayList(50, 890, 1550, 4990));
            CompositeColumnFilter ccf4 = newValListFilterLong("long_field", Lists.newArrayList(1L, 200L, 1500L, 4900L));
            CompositeColumnFilter ccf5 = newValListFilterInt("int_field1", Lists.newArrayList(50, 890, 1550, 4990));

            closer.addAll(ccf1, ccf2, ccf3, ccf4, ccf5);

            RuntimeFilter filter1 = new RuntimeFilter(pccf1, Lists.newArrayList(ccf1, ccf2), null);
            RuntimeFilter filter2 = new RuntimeFilter(pccf2, Lists.newArrayList(ccf3, ccf4), null);
            RuntimeFilter filter3 = new RuntimeFilter(pccf3, Lists.newArrayList(ccf1, ccf2), null);
            RuntimeFilter filter4 = new RuntimeFilter(null, Lists.newArrayList(ccf1, ccf2), null);
            RuntimeFilter filter5 = new RuntimeFilter(pccf1, Lists.newArrayList(ccf1), null);
            RuntimeFilter filter6 = new RuntimeFilter(pccf1, Lists.newArrayList(ccf1, ccf5), null);

            assertTrue(filter1.isOnSameColumns(filter2));
            assertTrue(filter2.isOnSameColumns(filter1));

            assertFalse(filter1.isOnSameColumns(filter3));
            assertFalse(filter3.isOnSameColumns(filter1));

            assertFalse(filter1.isOnSameColumns(filter4));
            assertFalse(filter4.isOnSameColumns(filter1));

            assertFalse(filter1.isOnSameColumns(filter5));
            assertFalse(filter5.isOnSameColumns(filter1));

            assertFalse(filter1.isOnSameColumns(filter6));
            assertFalse(filter6.isOnSameColumns(filter1));
        }
    }

    private CompositeColumnFilter newBloomFilter(List<String> columns) {
        BloomFilter bloomFilter = mock(BloomFilter.class);
        return new CompositeColumnFilter.Builder()
                .setColumnsList(columns)
                .setBloomFilter(bloomFilter)
                .setFilterType(CompositeColumnFilter.RuntimeFilterType.BLOOM_FILTER)
                .build();
    }

    private CompositeColumnFilter newValListFilterInt(String col1, List<Integer> vals) throws Exception {
        byte blockSize = Integer.BYTES;
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, vals.size(), blockSize, false);
             ArrowBuf keyBuf = testAllocator.buffer(blockSize)) {
            builder.setup();
            builder.setName("Test").setFieldType(Types.MinorType.INT, (byte) 0, (byte) 0).setFieldName(col1);
            for (int v : vals) {
                keyBuf.setInt(0, v);
                builder.insert(keyBuf);
            }

            return new CompositeColumnFilter.Builder()
                    .setFilterType(CompositeColumnFilter.RuntimeFilterType.VALUE_LIST)
                    .setColumnsList(Collections.singletonList(col1))
                    .setValueList(builder.build()).build();
        }
    }

    private CompositeColumnFilter newValListFilterLong(String col1, List<Long> vals) throws Exception {
        return newValListFilterLong(col1, vals, Types.MinorType.BIGINT);
    }

    private CompositeColumnFilter newValListFilterLong(String col1, List<Long> vals, Types.MinorType type) throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(testAllocator, vals.size(), (byte) 8, false);
             ArrowBuf keyBuf = testAllocator.buffer(8)) {
            builder.setup();
            builder.setName("Test").setFieldType(type, (byte) 0, (byte) 0).setFieldName(col1);
            for (long v : vals) {
                keyBuf.setLong(0, v);
                builder.insert(keyBuf);
            }

            return new CompositeColumnFilter.Builder()
                    .setFilterType(CompositeColumnFilter.RuntimeFilterType.VALUE_LIST)
                    .setColumnsList(Collections.singletonList(col1))
                    .setValueList(builder.build()).build();
        }
    }
}
