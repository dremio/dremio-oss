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
package com.dremio.exec.store.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.arrow.vector.SimpleIntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecTest;
import com.google.common.collect.ImmutableList;

public class TestPositionalDeleteFilter extends ExecTest {

  private static final int BATCH_SIZE = 5;

  private final List<AutoCloseable> closeables = new ArrayList<>();
  private SimpleIntVector deltas;

  @Before
  public void setup() {
    deltas = new SimpleIntVector("pos", getAllocator());
    closeables.add(deltas);
    deltas.allocateNew(BATCH_SIZE);
  }

  @After
  public void cleanup() throws Exception {
    AutoCloseables.close(closeables);
  }

  @Test
  public void testNoDeletes() {
    // [ ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
      .limit(0)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 8, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, 8, ImmutableList.of(0, 0, 0));
    verifyBatch(filter, 13, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
  }

  @Test
  public void testSingleContiguousRangeAtStart() {
    // [ 0 .. 99 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
      .limit(100)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 72, ImmutableList.of());
    verifyBatch(filter, ImmutableList.of(28, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
  }

  @Test
  public void testSingleContiguousRangeAtOffset() {
    // [ 8 .. 107 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(8L, i -> i + 1)
      .limit(100)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 72, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, 72, ImmutableList.of(0, 0, 0));
    verifyBatch(filter, ImmutableList.of(36, 0, 0, 0, 0));
  }

  @Test
  public void testAlternating() {
    // [ 0, 2, 4, 6, 8, 10, 12, 14 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
      .limit(16)
      .filter(i -> i % 2 == 0)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 8, ImmutableList.of(1, 1, 1, 1));
    verifyBatch(filter, 11, ImmutableList.of(1));
    verifyBatch(filter, ImmutableList.of(0, 1, 1, 0, 0));
  }

  @Test
  public void testOddAlternatingWithStartOffset() {
    // [ 3, 5, 7, 9, 11, 13, 15, 17 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(2L, i -> i + 1)
      .limit(16)
      .filter(i -> i % 2 == 1)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, ImmutableList.of(0, 0, 0, 1, 1));
    verifyBatch(filter, ImmutableList.of(1, 1, 1, 1, 1));
    verifyBatch(filter, ImmutableList.of(1, 0, 0, 0, 0));
  }


  @Test
  public void testAlternatingRunsOfLength2() {
    // [ 0, 1, 4, 5, 8, 9, 12, 13 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
      .limit(16)
      .filter(i -> i % 4 < 2)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 9, ImmutableList.of(2, 0, 2, 0));
    verifyBatch(filter, ImmutableList.of(1, 0, 2, 0, 0));
  }

  @Test
  public void testAlternatingRunsOfLength7() {
    // [ 0, 1, 2, 3, 4, 5, 6, 14, 15, 16, 17, 18, 19, 20, 28, 29, 30, 31, 32, 33, 34, 42, 43, 44, 45, 46, 47, 48 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
      .limit(7 * 8)
      .filter(i -> i % 14 < 7)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 29, ImmutableList.of(7, 0, 0, 0, 0));
    verifyBatch(filter, 29, ImmutableList.of(0, 0, 7, 0, 0));
    verifyBatch(filter, 29, ImmutableList.of(0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(6, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 7, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
  }

  @Test
  public void testSparseDeletes() {
    // [ 8, 16, 24 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(8L, i -> i + 8)
      .limit(3)
      .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 1, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(1, 0, 0, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 1, 0, 0));
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));
  }

  @Test
  public void testDuplicatePositions() {
    List<Long> input = ImmutableList.of(1L, 3L, 3L, 5L, 7L, 7L, 9L);
    PositionalDeleteIterator iterator = fromIterator(input.iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(iterator);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, ImmutableList.of(0, 1, 1, 1, 1));
    verifyBatch(filter, ImmutableList.of(1, 0, 0, 0, 0));
  }

  @Test
  public void testUnsortedDeleteIteratorFails() {
    List<Long> input = ImmutableList.of(1L, 3L, 2L, 4L);
    PositionalDeleteIterator iterator = fromIterator(input.iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(iterator);
    closeables.add(filter);
    filter.seek(0);

    assertThatThrownBy(() -> filter.applyToDeltas(Integer.MAX_VALUE, BATCH_SIZE, deltas))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Current row position should never be greater than next delete position");
  }

  @Test
  public void testEndRowLimitingWithNoDeletes() {
    // [ ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
        .limit(0)
        .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 8, ImmutableList.of(0, 0, 0, 0, 0));
    verifyBatch(filter, 8, ImmutableList.of(0, 0, 0));
  }

  @Test
  public void testSeek() {
    // [ 0 .. 99 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
        .limit(100)
        .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(50);

    verifyBatch(filter, ImmutableList.of(50, 0, 0, 0, 0));
  }

  @Test
  public void testSeekAfterIterationComplete() {
    Supplier<PositionalDeleteIterator> supplier = mock(Supplier.class);
    when(supplier.get()).thenReturn(fromIterator(ImmutableList.of(0L, 1L, 2L, 3L).iterator()));

    // [ 0 .. 3 ]
    PositionalDeleteFilter filter = new PositionalDeleteFilter(supplier, 1);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 8, ImmutableList.of(4, 0, 0, 0));
    filter.seek(11);
    verifyBatch(filter, ImmutableList.of(0, 0, 0, 0, 0));

    // supplier.get() should only be called once
    verify(supplier, times(1)).get();
  }

  @Test
  public void testSeekAfterPartialIteration() {
    // [ 0, 2, 4, 6, 8, 10, 12, 14 ]
    PositionalDeleteIterator input = fromIterator(Stream.iterate(0L, i -> i + 1)
        .limit(16)
        .filter(i -> i % 2 == 0)
        .iterator());

    PositionalDeleteFilter filter = new PositionalDeleteFilter(input);
    closeables.add(filter);
    filter.seek(0);

    verifyBatch(filter, 8, ImmutableList.of(1, 1, 1, 1));
    filter.seek(11);
    verifyBatch(filter, ImmutableList.of(0, 1, 1, 0, 0));
  }

  private void verifyBatch(PositionalDeleteFilter filter, List<Integer> expected) {
    verifyBatch(filter, Integer.MAX_VALUE, expected);
  }

  private void verifyBatch(PositionalDeleteFilter filter, long endRowPos, List<Integer> expected) {
    filter.applyToDeltas(endRowPos, BATCH_SIZE, deltas);

    List<Integer> actual = new ArrayList<>();
    for (int i = 0; i < deltas.getValueCount(); i++) {
      actual.add(deltas.get(i));
    }

    assertThat(actual).isEqualTo(expected);
  }

  private PositionalDeleteIterator fromIterator(Iterator<Long> iterator) {
    return new PositionalDeleteIterator() {
      @Override
      public void close() throws Exception {
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Long next() {
        return iterator.next();
      }
    };
  }
}
