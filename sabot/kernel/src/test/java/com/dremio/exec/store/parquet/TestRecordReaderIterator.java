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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;

/**
 * Tests for {@link RecordReaderIterator}
 */
public class TestRecordReaderIterator {

    @Test
    public void testFromSingleReader() {
        RecordReader rr = mock(RecordReader.class);
        RecordReaderIterator it = RecordReaderIterator.from(rr);

        assertTrue(it.hasNext());
        assertEquals(rr, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testFromIterator() {
        List<RecordReader> readers = IntStream.range(0, 10).mapToObj(i -> mock(RecordReader.class)).collect(Collectors.toList());
        RecordReaderIterator it = RecordReaderIterator.from(readers.iterator());

        for (int i = 0; i < 10; i++) {
            assertTrue(it.hasNext());
            assertEquals(readers.get(i), it.next());
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testJoinBothIteratorsHaveValues() throws Exception {
        List<RecordReader> readers1 = IntStream.range(0, 10).mapToObj(i -> mock(RecordReader.class)).collect(Collectors.toList());
        List<RecordReader> readers2 = IntStream.range(0, 10).mapToObj(i -> mock(RecordReader.class)).collect(Collectors.toList());

        RecordReaderIterator it1 = spy(RecordReaderIterator.from(readers1.iterator()));
        RecordReaderIterator it2 = spy(RecordReaderIterator.from(readers2.iterator()));

        RecordReaderIterator joinedIt = RecordReaderIterator.join(it1, it2);
        Set<RecordReader> returnedValues = new HashSet<>();
        while(joinedIt.hasNext()) {
            returnedValues.add(joinedIt.next());
        }

        Set<RecordReader> expectedValues = new HashSet<>(readers1);
        expectedValues.addAll(readers2);
        assertEquals(expectedValues, returnedValues);

        RuntimeFilter filter = mock(RuntimeFilter.class);
        joinedIt.addRuntimeFilter(filter);

        verify(it1).addRuntimeFilter(eq(filter));
        verify(it2).addRuntimeFilter(eq(filter));

        joinedIt.close();
        verify(it1).close();
        verify(it2).close();
    }

    @Test
    public void testJoinOneIteratorsHaveValues() throws Exception {
        List<RecordReader> readers1 = IntStream.range(0, 10).mapToObj(i -> mock(RecordReader.class)).collect(Collectors.toList());
        List<RecordReader> readers2 = Collections.EMPTY_LIST;

        RecordReaderIterator it1 = spy(RecordReaderIterator.from(readers1.iterator()));
        RecordReaderIterator it2 = spy(RecordReaderIterator.from(readers2.iterator()));

        RecordReaderIterator joinedIt = RecordReaderIterator.join(it1, it2);
        Set<RecordReader> returnedValues = new HashSet<>();
        while(joinedIt.hasNext()) {
            returnedValues.add(joinedIt.next());
        }

        Set<RecordReader> expectedValues = new HashSet<>(readers1);
        expectedValues.addAll(readers2);
        assertEquals(expectedValues, returnedValues);

        RuntimeFilter filter = mock(RuntimeFilter.class);
        joinedIt.addRuntimeFilter(filter);

        verify(it1).addRuntimeFilter(eq(filter));
        verify(it2).addRuntimeFilter(eq(filter));

        joinedIt.close();
        verify(it1).close();
        verify(it2).close();
    }
}
