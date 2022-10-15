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

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.test.DremioTest;

public class TestComparisonUtils extends DremioTest {

  @Test
  public void testByteArrayComparatorEqualArrays() {
    // ARRANGE
    final byte[] a1 = new byte[] { 2, 2 };
    final byte[] a2 = new byte[] { 2, 2 };

    // ACT
    final int comparisonResult = ComparisonUtils.compareByteArrays(a1, a2);

    // ASSERT
    assertEquals(0, comparisonResult);
  }

  @Test
  public void testByteArrayComparatorSortableArrays() {
    // ARRANGE
    final byte[] a1 = new byte[] { 2, 2 };
    final byte[] a2 = new byte[] { 2, 3 };

    // ACT
    final int comparisonResult = ComparisonUtils.compareByteArrays(a1, a2);

    // ASSERT
    assertEquals(-1, comparisonResult);
  }

  @Test
  public void testByteArrayComparatorDifferentLength() {
    // ARRANGE
    final byte[] a1 = new byte[] { 2, 2 };
    final byte[] a2 = new byte[] { 2, 2, 2 };

    // ACT
    final int comparisonResult = ComparisonUtils.compareByteArrays(a1, a2);

    // ASSERT
    assertEquals(-1, comparisonResult);
  }
}
