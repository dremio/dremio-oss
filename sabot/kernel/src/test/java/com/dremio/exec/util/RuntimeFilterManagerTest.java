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

package com.dremio.exec.util;

import static com.dremio.exec.proto.ExecProtos.RuntimeFilterType.BLOOM_FILTER;
import static com.dremio.exec.proto.ExecProtos.RuntimeFilterType.VALUE_LIST;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.util.RuntimeFilterManager.RuntimeFilterManagerEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests for {@link RuntimeFilterManager}
 */
public class RuntimeFilterManagerTest {
    private static final int MAX_VALS = 1000;

    private BufferAllocator allocator = new RootAllocator();

    private final int majorFragment1 = 1;
    private final int opId1 = 101;
    private final int majorFragment2 = 2;
    private final int opId2 = 202;

    @Test
    public void testPartitionColMergeSingleProbe() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(any(BufferAllocator.class))).thenReturn(copyBf);

        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.of(bf), EMPTY_LIST, 1);
        assertFalse(entry1.isComplete());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, Optional.of(mockedBloom()), EMPTY_LIST, 2);
        assertFalse(entry2.isComplete());
        assertEquals(entry1, entry2);
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, Optional.of(mockedBloom()), EMPTY_LIST, 3);
        assertTrue(entry1.isComplete());
        assertFalse(entry1.isDropped());
        assertEquals(entry1, entry3);

        verify(copyBf, times(2)).merge(any(BloomFilter.class));
        verify(copyBf, times(2)).isCrossingMaxFPP();
    }

    @Test
    public void testNonPartitionColMergeSingleProbe() throws Exception {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, EMPTY_LIST, Lists.newArrayList("col1", "col2"));
        ValueListFilter vA1 = newValListFilter("col1", Lists.newArrayList(4, 1, 3, 2));
        ValueListFilter vA2 = newValListFilter("col2", Lists.newArrayList(11, 12, 13, 14));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.empty(), Lists.newArrayList(vA1, vA2), 1);
        assertFalse(entry1.isComplete());
        assertFalse(entry1.isDropped());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, EMPTY_LIST, Lists.newArrayList("col1", "col2"));
        ValueListFilter vB1 = newValListFilter("col1", Lists.newArrayList(6, 4, 5, 3));
        ValueListFilter vB2 = newValListFilter("col2", Lists.newArrayList(15, 16, 17, 18));
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, Optional.empty(), Lists.newArrayList(vB1, vB2), 2);
        assertFalse(entry2.isComplete());
        assertFalse(entry1.isDropped());
        assertEquals(entry1, entry2);
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1, EMPTY_LIST, Lists.newArrayList("col1", "col2"));
        ValueListFilter vC1 = newValListFilter("col1", Lists.newArrayList(6, 9, 8, 3));
        ValueListFilter vC2 = newValListFilter("col2", Lists.newArrayList(19, 20, 21, 22));
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, Optional.empty(), Lists.newArrayList(vC1, vC2), 3);
        assertTrue(entry1.isComplete());
        assertFalse(entry1.isDropped());
        assertEquals(entry1, entry3);

        ValueListFilter vR1 = entry1.getNonPartitionColFilter("col1");
        List<Integer> expectedVR1 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 8, 9);
        assertEquals(expectedVR1.size(), vR1.getValueCount());
        List<Integer> actualList1 = new ArrayList<>(vR1.getValueCount());
        IntStream.range(0, vR1.getValueCount()).forEach(i -> actualList1.add(vR1.valOnlyBuf().getInt(i * 4)));
        assertEquals(expectedVR1, actualList1);

        ValueListFilter vR2 = entry1.getNonPartitionColFilter("col2");
        List<Integer> expectedVR2 = IntStream.range(11, 23).mapToObj(i -> i).collect(Collectors.toList());
        assertEquals(expectedVR2.size(), vR2.getValueCount());
        List<Integer> actualList2 = new ArrayList<>(vR1.getValueCount());
        IntStream.range(0, vR2.getValueCount()).forEach(i -> actualList2.add(vR2.valOnlyBuf().getInt(i * 4)));
        assertEquals(expectedVR2, actualList2);

        // No change to incoming buffers refcount
        assertEquals(1, vA1.buf().refCnt());
        assertEquals(1, vA2.buf().refCnt());
        assertEquals(1, vB1.buf().refCnt());
        assertEquals(1, vB2.buf().refCnt());
        assertEquals(1, vC1.buf().refCnt());
        assertEquals(1, vC2.buf().refCnt());

        // Retain the base buffers
        assertEquals(1, vR1.buf().refCnt());
        assertEquals(1, vR2.buf().refCnt());

        AutoCloseables.close(vA1, vA2, vB1, vB2, vC1, vC2);
        AutoCloseables.close(entry1);
    }

    @Test
    public void testMultiCategoryColMerge() throws Exception {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(any(BufferAllocator.class))).thenReturn(copyBf);

        when(copyBf.getNumBitsSet()).thenReturn(10L);
        ValueListFilter vA1 = newValListFilter("col3", Lists.newArrayList(4, 1, 3, 2));
        ValueListFilter vA2 = newValListFilter("col4", Lists.newArrayList(11, 12, 13, 14));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.of(bf), Lists.newArrayList(vA1, vA2), 1);
        assertFalse(entry1.isComplete());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1,  Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vB1 = newValListFilter("col3", Lists.newArrayList(6, 4, 5, 3));
        ValueListFilter vB2 = newValListFilter("col4", Lists.newArrayList(15, 16, 17, 18));
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, Optional.of(bf), Lists.newArrayList(vB1, vB2), 2);        assertFalse(entry2.isComplete());
        assertEquals(entry1, entry2);
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1,  Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vC1 = newValListFilter("col3", Lists.newArrayList(6, 9, 8, 3));
        ValueListFilter vC2 = newValListFilter("col4", Lists.newArrayList(19, 20, 21, 22));
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, Optional.of(bf), Lists.newArrayList(vC1, vC2), 3);
        assertTrue(entry1.isComplete());
        assertFalse(entry1.isDropped());
        assertEquals(entry1, entry3);

        verify(copyBf, times(2)).merge(any(BloomFilter.class));
        verify(copyBf, times(2)).isCrossingMaxFPP();
        ValueListFilter vR1 = entry1.getNonPartitionColFilter("col3");
        List<Integer> expectedVR1 = Lists.newArrayList(1, 2, 3, 4, 5, 6, 8, 9);
        assertEquals(expectedVR1.size(), vR1.getValueCount());
        List<Integer> actualList1 = new ArrayList<>(vR1.getValueCount());
        IntStream.range(0, vR1.getValueCount()).forEach(i -> actualList1.add(vR1.valOnlyBuf().getInt(i * 4)));
        assertEquals(expectedVR1, actualList1);

        ValueListFilter vR2 = entry1.getNonPartitionColFilter("col4");
        List<Integer> expectedVR2 = IntStream.range(11, 23).mapToObj(i -> i).collect(Collectors.toList());
        assertEquals(expectedVR2.size(), vR2.getValueCount());
        List<Integer> actualList2 = new ArrayList<>(vR1.getValueCount());
        IntStream.range(0, vR2.getValueCount()).forEach(i -> actualList2.add(vR2.valOnlyBuf().getInt(i * 4)));
        assertEquals(expectedVR2, actualList2);
        AutoCloseables.close(vR1, vR2);

        RuntimeFilter protoFilter = entry1.getCompositeFilter();
        assertEquals(entry1.getPartitionColFilter().getNumBitsSet(), protoFilter.getPartitionColumnFilter().getValueCount());
        assertEquals(entry1.getNonPartitionColFilter("col3").getValueCount(), protoFilter.getNonPartitionColumnFilter(0).getValueCount());
        assertEquals(entry1.getNonPartitionColFilter("col3").getSizeInBytes(), protoFilter.getNonPartitionColumnFilter(0).getSizeBytes());
        assertEquals(entry1.getNonPartitionColFilter("col4").getValueCount(), protoFilter.getNonPartitionColumnFilter(1).getValueCount());
        assertEquals(entry1.getNonPartitionColFilter("col4").getSizeInBytes(), protoFilter.getNonPartitionColumnFilter(1).getSizeBytes());
    }

    @Test
    public void testFilterMergeMultiProbeTargets() throws Exception{
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2));

        BloomFilter bf1 = mockedBloom();
        BloomFilter copyBf1 = mockedBloom();
        when(bf1.createCopy(any(BufferAllocator.class))).thenReturn(copyBf1);
        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("colT1"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.of(bf1), EMPTY_LIST, 1);

        BloomFilter bf2 = mockedBloom();
        BloomFilter copyBf2 = mockedBloom();
        when(bf2.createCopy(any(BufferAllocator.class))).thenReturn(copyBf2);
        RuntimeFilter frMinor1Target2 = newFilter(opId2, majorFragment2, Lists.newArrayList("colT2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor1Target2, Optional.of(bf2), EMPTY_LIST, 1);
        assertNotEquals(entry1, entry2);

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("colT1"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor2Target1, Optional.of(mockedBloom()), EMPTY_LIST, 2);
        assertEquals(entry1, entry3);
        assertTrue(entry1.isComplete());
        assertFalse(entry1.isDropped());
        assertFalse(entry2.isComplete());

        RuntimeFilter frMinor2Target2 = newFilter(opId2, majorFragment2, Lists.newArrayList("colT2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry4 = filterManager.coalesce(frMinor2Target2, Optional.of(mockedBloom()), EMPTY_LIST, 2);
        assertEquals(entry2, entry4);
        assertTrue(entry2.isComplete());
        assertFalse(entry2.isDropped());

        verify(copyBf1, times(1)).merge(any(BloomFilter.class));
        verify(copyBf1, times(1)).isCrossingMaxFPP();

        verify(copyBf2, times(1)).merge(any(BloomFilter.class));
        verify(copyBf2, times(1)).isCrossingMaxFPP();
    }

    @Test
    public void testRuntimeFilterDropCapReached() throws Exception {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(eq(allocator))).thenReturn(copyBf);

        ValueListFilter vA1 = newValListFilter("col3", Lists.newArrayList(4, 1, 3, 2));
        ValueListFilter vA2 = newValListFilter("col4", Lists.newArrayList(11, 12, 13, 14));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.of(bf), Lists.newArrayList(vA1, vA2), 1);
        assertFalse(entry1.isComplete());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());
        when(copyBf.isCrossingMaxFPP()).thenReturn(true);

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vB1 = newValListFilter("col3", Lists.newArrayList(6, 4, 5, 3));
        ValueListFilter vB2 = newValListFilter("col4", Lists.newArrayList(15, 16, 17, 18));
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, Optional.of(bf), Lists.newArrayList(vB1, vB2), 2);
        assertFalse(entry2.isComplete());
        assertEquals(entry1, entry2);
        assertFalse(entry1.isDropped());
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vC1 = newValListFilter("col3", IntStream.range(0, 10_000).mapToObj(i -> i).collect(Collectors.toList()));
        ValueListFilter vC2 = newValListFilter("col4", IntStream.range(0, 10_000).mapToObj(i -> i).collect(Collectors.toList()));
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, Optional.of(bf), Lists.newArrayList(vC1, vC2), 3);
        assertTrue(entry3.isComplete());
        assertTrue(entry3.isDropped());
    }

    @Test
    public void testPartialFilterDrop() throws Exception {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(eq(allocator))).thenReturn(copyBf);

        ValueListFilter vA1 = newValListFilter("col3", Lists.newArrayList(4, 1, 3, 2));
        ValueListFilter vA2 = newValListFilter("col4", Lists.newArrayList(11, 12, 13, 14));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, Optional.of(bf), Lists.newArrayList(vA1, vA2), 1);
        assertFalse(entry1.isComplete());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());
        when(copyBf.isCrossingMaxFPP()).thenReturn(true);

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vB1 = newValListFilter("col3", Lists.newArrayList(6, 4, 5, 3));
        ValueListFilter vB2 = newValListFilter("col4", Lists.newArrayList(15, 16, 17, 18));
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, Optional.of(bf), Lists.newArrayList(vB1, vB2), 2);
        assertFalse(entry2.isComplete());
        assertEquals(entry1, entry2);
        assertFalse(entry1.isDropped());
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), Lists.newArrayList("col3", "col4"));
        ValueListFilter vC1 = newValListFilter("col3", IntStream.range(0, 10_000).mapToObj(i -> i).collect(Collectors.toList()));
        ValueListFilter vC2 = newValListFilter("col4", IntStream.range(0, 10).mapToObj(i -> i).collect(Collectors.toList()));
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, Optional.of(bf), Lists.newArrayList(vC1, vC2), 3);
        assertTrue(entry3.isComplete());
        assertFalse(entry3.isDropped());

        assertNull(entry1.getNonPartitionColFilter("col3"));
        assertNotNull(entry1.getNonPartitionColFilter("col4"));
        assertNull(entry1.getPartitionColFilter());

        RuntimeFilter filterMsg = entry1.getCompositeFilter();
        assertFalse(filterMsg.hasPartitionColumnFilter());
        assertTrue(filterMsg.getNonPartitionColumnFilterList().size() == 1);
        assertEquals("col4", filterMsg.getNonPartitionColumnFilterList().get(0).getColumns(0));
        filterManager.close();
    }

    @Test
    public void testFilterDropDueToFppTolerance() throws Exception {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));

        RuntimeFilter filter1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(any(BufferAllocator.class))).thenReturn(copyBf);

        when(copyBf.isCrossingMaxFPP()).thenReturn(true);
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(filter1, Optional.of(bf), EMPTY_LIST, 1);

        RuntimeFilter filter2 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(filter2, Optional.of(mockedBloom()), EMPTY_LIST, 2);

        assertTrue(entry1.isDropped());
        assertEquals(entry1, entry2);
        verify(copyBf, times(1)).isCrossingMaxFPP();
        filterManager.remove(entry1);
        assertEquals(1, filterManager.getFilterDropCount());
    }

    @Test
    public void testDropFilterDueToMergeFailure() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(allocator, MAX_VALS, Sets.newHashSet(1, 2, 3));
        RuntimeFilter filter1 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        BloomFilter bf = mockedBloom();
        BloomFilter copyBf = mockedBloom();
        when(bf.createCopy(any(BufferAllocator.class))).thenReturn(copyBf);

        doThrow(new IllegalArgumentException("Incompatible BloomFilter, different hashing technique.")).when(copyBf).merge(any(BloomFilter.class));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(filter1, Optional.of(bf), EMPTY_LIST, 1);

        RuntimeFilter filter2 = newFilter(opId1, majorFragment1, Lists.newArrayList("col1", "col2"), EMPTY_LIST);
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(filter2, Optional.of(mockedBloom()), EMPTY_LIST, 2);

        assertTrue(entry1.isDropped());
        assertEquals(entry1, entry2);
        verify(copyBf, times(1)).merge(any(BloomFilter.class));
        filterManager.remove(entry1);
        assertEquals(1, filterManager.getFilterDropCount());
    }

    private BloomFilter mockedBloom() {
        BloomFilter bloom = mock(BloomFilter.class);
        when(bloom.isCrossingMaxFPP()).thenReturn(false);
        doNothing().when(bloom).merge(any(BloomFilter.class));
        return bloom;
    }

    private RuntimeFilter newFilter(int opId, int majorFragment, List<String> partitionColNames,
                                    List<String> nonPartitionColNames) {
        CompositeColumnFilter partitionColFilter = CompositeColumnFilter.newBuilder()
                .setFilterType(BLOOM_FILTER)
                .addAllColumns(partitionColNames)
                .setSizeBytes(64)
                .build();

        List<CompositeColumnFilter> nonPartitionColFilters = nonPartitionColNames.stream().map(f -> {
            CompositeColumnFilter npColFilter = CompositeColumnFilter.newBuilder()
                    .setFilterType(VALUE_LIST)
                    .addAllColumns(Lists.newArrayList(f))
                    .setSizeBytes(64).build();
            return npColFilter;
        }).collect(Collectors.toList());

        RuntimeFilter runtimeFilter = RuntimeFilter.newBuilder()
                .setProbeScanMajorFragmentId(majorFragment)
                .setProbeScanOperatorId(opId)
                .setPartitionColumnFilter(partitionColFilter)
                .addAllNonPartitionColumnFilter(nonPartitionColFilters).build();
        return runtimeFilter;
    }

    private ValueListFilter newValListFilter(String col1, List<Integer> vals) throws Exception {
        try (ValueListFilterBuilder builder = new ValueListFilterBuilder(allocator, vals.size(), (byte) 4, false);
             ArrowBuf keyBuf = allocator.buffer(4)) {
            builder.setup();
            builder.setName("Test").setFieldType(Types.MinorType.INT, (byte) 0, (byte) 0).setFieldName(col1);
            for (int v : vals) {
                keyBuf.setInt(0, v);
                builder.insert(keyBuf);
            }
            return builder.build();
        }
    }
}
