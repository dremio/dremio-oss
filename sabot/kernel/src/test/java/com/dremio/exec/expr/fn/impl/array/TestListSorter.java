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
package com.dremio.exec.expr.fn.impl.array;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestListSorter {
  public static final String TEST = "test";
  @Mock private ArrowBuf sortIndexes;
  @Mock private FieldReader reader;

  private ArrayHelper.ListSorter sorter;

  @Before
  public void before() {
    sorter = new ArrayHelper.ListSorter(sortIndexes, reader, 0);
  }

  @Test
  public void testCompare() {
    assertThat(sorter.compare(1, 2)).isLessThan(0);
    assertThat(sorter.compare(2, 1)).isGreaterThan(0);
    assertThat(sorter.compare(null, 1)).isLessThan(0);
    assertThat(sorter.compare(1, null)).isGreaterThan(0);
  }

  @Test
  public void testSetIndexMap() {
    sorter.setIndexMap(0, 10);
    sorter.setIndexMap(1, 20);
    sorter.setIndexMap(2, 30);
    verify(sortIndexes).setInt(0, 10);
    verify(sortIndexes).setInt(4, 20);
    verify(sortIndexes).setInt(8, 30);
  }

  @Test
  public void testMapIndex() {
    sorter.mapIndex(0);
    sorter.mapIndex(1);
    sorter.mapIndex(2);
    verify(sortIndexes).getInt(0);
    verify(sortIndexes).getInt(4);
    verify(sortIndexes).getInt(8);
  }

  @Test
  public void testReadObject() {
    when(sortIndexes.getInt(anyLong())).thenReturn(10);
    when(reader.readObject()).thenReturn(TEST);
    assertThat(sorter.readObject(5)).isEqualTo(TEST);
    verify(sortIndexes).getInt(20);
    verify(reader).setPosition(10);
    verify(reader).readObject();
  }

  @Test
  public void testReadObjectText() {
    when(sortIndexes.getInt(anyLong())).thenReturn(10);
    when(reader.readObject()).thenReturn(new org.apache.arrow.vector.util.Text(TEST));
    assertThat(sorter.readObject(5)).isEqualTo(TEST);
    verify(sortIndexes).getInt(20);
    verify(reader).setPosition(10);
    verify(reader).readObject();
  }

  @Test
  public void testFindMedian() {
    when(sortIndexes.getInt(anyLong())).thenReturn(10, 20, 30);
    when(reader.readObject()).thenReturn(1, 2, 3);
    assertThat(sorter.findMedian(0, 2)).isEqualTo(1);
    verify(sortIndexes).getInt(0);
    verify(sortIndexes).getInt(4);
    verify(sortIndexes).getInt(8);
    verify(reader).setPosition(10);
    verify(reader).setPosition(20);
    verify(reader).setPosition(30);
    verify(reader, times(3)).readObject();
  }

  @Test
  public void testFindMedian2() {
    when(sortIndexes.getInt(anyLong())).thenReturn(10, 20, 30);
    when(reader.readObject()).thenReturn(3, 1, 2);
    assertThat(sorter.findMedian(0, 2)).isEqualTo(2);
    verify(sortIndexes).getInt(0);
    verify(sortIndexes).getInt(4);
    verify(sortIndexes).getInt(8);
    verify(reader).setPosition(10);
    verify(reader).setPosition(20);
    verify(reader).setPosition(30);
    verify(reader, times(3)).readObject();
  }

  @Test
  public void testSwapIndexes() {
    when(sortIndexes.getInt(4)).thenReturn(10);
    when(sortIndexes.getInt(20)).thenReturn(50);
    sorter.swapIndexes(1, 5);
    verify(sortIndexes).setInt(4, 50);
    verify(sortIndexes).setInt(20, 10);
  }
}
