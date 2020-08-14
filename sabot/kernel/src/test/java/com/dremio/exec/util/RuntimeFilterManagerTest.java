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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.dremio.exec.proto.ExecProtos.CompositeColumnFilter;
import com.dremio.exec.proto.ExecProtos.RuntimeFilter;
import com.dremio.exec.util.RuntimeFilterManager.RuntimeFilterManagerEntry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests for {@link RuntimeFilterManager}
 */
public class RuntimeFilterManagerTest {

    private final int majorFragment1 = 1;
    private final int opId1 = 101;
    private final int majorFragment2 = 2;
    private final int opId2 = 202;

    @Test
    public void testFilterMergeSingleProbeTarget() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(Sets.newHashSet(1, 2, 3));

        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, "col1", "col2");
        BloomFilter bf = mockedBloom();
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, bf, 1);
        assertFalse(entry1.isComplete());
        assertEquals(Sets.newHashSet(2, 3), entry1.getRemainingMinorFragments());

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, "col1", "col2");
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor2Target1, mockedBloom(), 2);
        assertFalse(entry2.isComplete());
        assertEquals(entry1, entry2);
        assertEquals(Sets.newHashSet(3), entry2.getRemainingMinorFragments());

        RuntimeFilter frMinor3Target1 = newFilter(opId1, majorFragment1, "col1", "col2");
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor3Target1, mockedBloom(), 3);
        assertTrue(entry1.isComplete());
        assertTrue(entry1.isNotDropped());
        assertEquals(entry1, entry3);

        verify(bf, times(2)).merge(any(BloomFilter.class));
        verify(bf, times(2)).isCrossingMaxFPP();
    }

    @Test
    public void testFilterMergeMultiProbeTargets() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(Sets.newHashSet(1, 2));

        BloomFilter bf1 = mockedBloom();
        RuntimeFilter frMinor1Target1 = newFilter(opId1, majorFragment1, "colT1");
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(frMinor1Target1, bf1, 1);

        BloomFilter bf2 = mockedBloom();
        RuntimeFilter frMinor1Target2 = newFilter(opId2, majorFragment2, "colT2");
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(frMinor1Target2, bf2, 1);
        assertNotEquals(entry1, entry2);

        RuntimeFilter frMinor2Target1 = newFilter(opId1, majorFragment1, "colT1");
        RuntimeFilterManagerEntry entry3 = filterManager.coalesce(frMinor2Target1, mockedBloom(), 2);
        assertEquals(entry1, entry3);
        assertTrue(entry1.isComplete());
        assertTrue(entry1.isNotDropped());
        assertFalse(entry2.isComplete());

        RuntimeFilter frMinor2Target2 = newFilter(opId2, majorFragment2, "colT2");
        RuntimeFilterManagerEntry entry4 = filterManager.coalesce(frMinor2Target2, mockedBloom(), 2);
        assertEquals(entry2, entry4);
        assertTrue(entry2.isComplete());
        assertTrue(entry2.isNotDropped());

        verify(bf1, times(1)).merge(any(BloomFilter.class));
        verify(bf1, times(1)).isCrossingMaxFPP();

        verify(bf2, times(1)).merge(any(BloomFilter.class));
        verify(bf2, times(1)).isCrossingMaxFPP();
    }

    @Test
    public void testFilterDropDueToFppTolerance() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(Sets.newHashSet(1, 2, 3));

        RuntimeFilter filter1 = newFilter(opId1, majorFragment1, "col1", "col2");
        BloomFilter bf = mockedBloom();
        when(bf.isCrossingMaxFPP()).thenReturn(true);
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(filter1, bf, 1);

        RuntimeFilter filter2 = newFilter(opId1, majorFragment1, "col1", "col2");
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(filter2, mockedBloom(), 2);

        assertFalse(entry1.isNotDropped());
        assertEquals(entry1, entry2);
        verify(bf, times(1)).isCrossingMaxFPP();
        filterManager.remove(entry1);
        assertEquals(1, filterManager.getFilterDropCount());
    }

    @Test
    public void testDropFilterDueToMergeFailure() {
        RuntimeFilterManager filterManager = new RuntimeFilterManager(Sets.newHashSet(1, 2, 3));

        RuntimeFilter filter1 = newFilter(opId1, majorFragment1, "col1", "col2");
        BloomFilter bf = mockedBloom();
        doThrow(new IllegalArgumentException("Incompatible BloomFilter, different hashing technique.")).when(bf).merge(any(BloomFilter.class));
        RuntimeFilterManagerEntry entry1 = filterManager.coalesce(filter1, bf, 1);

        RuntimeFilter filter2 = newFilter(opId1, majorFragment1, "col1", "col2");
        RuntimeFilterManagerEntry entry2 = filterManager.coalesce(filter2, mockedBloom(), 2);

        assertFalse(entry1.isNotDropped());
        assertEquals(entry1, entry2);
        verify(bf, times(1)).merge(any(BloomFilter.class));
        filterManager.remove(entry1);
        assertEquals(1, filterManager.getFilterDropCount());
    }

    private BloomFilter mockedBloom() {
        BloomFilter bloom = mock(BloomFilter.class);
        when(bloom.isCrossingMaxFPP()).thenReturn(false);
        doNothing().when(bloom).merge(any(BloomFilter.class));
        return bloom;
    }

    private RuntimeFilter newFilter(int opId, int majorFragment, String... colNames) {
        CompositeColumnFilter colFilter = CompositeColumnFilter.newBuilder()
                .setFilterType(BLOOM_FILTER)
                .addAllColumns(Lists.newArrayList(colNames))
                .setSizeBytes(64)
                .build();
        return RuntimeFilter.newBuilder()
                .setProbeScanMajorFragmentId(majorFragment).setProbeScanOperatorId(opId)
                .setPartitionColumnFilter(colFilter).build();
    }
}
