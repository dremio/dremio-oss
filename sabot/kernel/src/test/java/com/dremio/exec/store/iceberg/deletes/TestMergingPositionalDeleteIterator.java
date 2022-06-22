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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestMergingPositionalDeleteIterator {

  @Test
  public void testIteration() {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.of(2L, 5L, 9L).iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.of(3L, 6L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    List<Long> expected = ImmutableList.of(2L, 3L, 5L, 6L, 9L);
    assertThat(iterator).toIterable().containsExactlyElementsOf(expected);
  }

  @Test
  public void testIterationWithDupes() {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.of(2L, 5L, 9L).iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.of(3L, 5L, 6L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    List<Long> expected = ImmutableList.of(2L, 3L, 5L, 5L, 6L, 9L);
    assertThat(iterator).toIterable().containsExactlyElementsOf(expected);
  }

  @Test
  public void testMergeReturnsSingleSourceIterator() {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.of(2L, 5L, 9L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1));

    assertThat(iterator).isSameAs(source1);
  }

  @Test
  public void testSourceIteratorsImmediatelyClosed() throws Exception {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.of(2L, 5L, 9L).iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.of(3L, 6L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    assertThat(iterator.next()).isEqualTo(2L);
    assertThat(iterator.next()).isEqualTo(3L);
    assertThat(iterator.next()).isEqualTo(5L);
    verify(source2, never()).close();
    assertThat(iterator.next()).isEqualTo(6L);
    verify(source2, times(1)).close();
    verify(source1, never()).close();
    assertThat(iterator.next()).isEqualTo(9L);
    verify(source1, times(1)).close();
    assertThat(iterator.hasNext()).isFalse();

    // close should be a no-op since both iterators reached their end
    iterator.close();
    verify(source1, times(1)).close();
    verify(source2, times(1)).close();
  }

  @Test
  public void testSourceIteratorsClosedIfNotFullyIterated() throws Exception {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.of(2L, 5L, 9L).iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.of(3L, 6L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    assertThat(iterator.next()).isEqualTo(2L);
    assertThat(iterator.next()).isEqualTo(3L);
    verify(source1, never()).close();
    verify(source2, never()).close();
    assertThat(iterator.hasNext()).isTrue();

    iterator.close();
    verify(source1, times(1)).close();
    verify(source2, times(1)).close();
  }

  @Test
  public void testSourceIteratorsClosedIfEmpty() throws Exception {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.<Long>of().iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.<Long>of().iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    // both iterators should have been closed in the constructor
    verify(source1, times(1)).close();
    verify(source2, times(1)).close();
    iterator.close();
    verify(source1, times(1)).close();
    verify(source2, times(1)).close();
  }

  @Test
  public void testEmptySourceIterator() {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.<Long>of().iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.of(3L, 4L).iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    List<Long> expected = ImmutableList.of(3L, 4L);
    assertThat(iterator).toIterable().containsExactlyElementsOf(expected);
  }

  @Test
  public void testAllSourceIteratorsEmpty() {
    PositionalDeleteIterator source1 = createPositionalDeleteIterator(ImmutableList.<Long>of().iterator());
    PositionalDeleteIterator source2 = createPositionalDeleteIterator(ImmutableList.<Long>of().iterator());
    PositionalDeleteIterator iterator = MergingPositionalDeleteIterator.merge(ImmutableList.of(source1, source2));

    List<Long> expected = ImmutableList.of();
    assertThat(iterator).toIterable().containsExactlyElementsOf(expected);
  }

  private PositionalDeleteIterator createPositionalDeleteIterator(Iterator<Long> iterator) {
    PositionalDeleteIterator positionalDeleteIterator = mock(PositionalDeleteIterator.class);
    when(positionalDeleteIterator.hasNext()).thenAnswer(invocation -> iterator.hasNext());
    when(positionalDeleteIterator.next()).thenAnswer(invocation -> iterator.next());
    return positionalDeleteIterator;
  }
}
