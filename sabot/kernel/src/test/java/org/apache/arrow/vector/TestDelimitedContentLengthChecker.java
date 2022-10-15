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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.dremio.test.DremioTest;

public class TestDelimitedContentLengthChecker extends DremioTest {
  @Test
  public void testSpaceCheckForFirstItem() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, false);

    // ASSERT
    assertTrue(checker.hasSpaceFor(6));
    assertFalse(checker.hasSpaceFor(7));
  }

  @Test
  public void testSpaceCheckForSubsequentItems() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, false);

    // ACT
    checker.addToTotalLength(2);
    checker.addToTotalLength(2);

    // ASSERT
    assertTrue(checker.hasSpaceFor(0));
    assertFalse(checker.hasSpaceFor(1));
  }

  @Test
  public void testReset() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, false);

    checker.addToTotalLength(6);
    assertFalse(checker.hasSpaceFor(6));

    // ACT
    checker.reset();

    // ASSERT
    assertTrue(checker.hasSpaceFor(6));
    assertFalse(checker.hasSpaceFor(7));
  }

  @Test
  public void testEmptyContentAdditions() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(2, 6, false);

    // ACT
    checker.addToTotalLength(0);
    checker.addToTotalLength(0);
    checker.addToTotalLength(0);
    checker.addToTotalLength(0);

    // ASSERT
    assertFalse(checker.hasSpaceFor(0));
  }

  @Test
  public void testNoAllowOverflow() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, false);

    // ACT
    checker.addToTotalLength(2);
    checker.addToTotalLength(2);

    // ASSERT
    assertFalse(checker.hasSpaceFor(2));
  }

  @Test
  public void testAllowOverflow() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, true);

    // ACT
    checker.addToTotalLength(2);
    checker.addToTotalLength(2);

    // ASSERT
    assertTrue(checker.hasSpaceFor(2));
    checker.addToTotalLength(20);

    assertFalse(checker.hasSpaceFor(2));
  }

  @Test
  public void testAllowOverflowWithReset() {
    // ARRANGE
    final DelimitedContentLengthChecker checker = new DelimitedContentLengthChecker(1, 6, true);

    // ACT
    checker.addToTotalLength(2);
    checker.addToTotalLength(2);

    // ASSERT
    assertTrue(checker.hasSpaceFor(2));
    checker.addToTotalLength(20);

    assertFalse(checker.hasSpaceFor(2));

    checker.reset();

    // ACT
    checker.addToTotalLength(2);
    checker.addToTotalLength(2);

    // ASSERT
    assertTrue(checker.hasSpaceFor(2));
    checker.addToTotalLength(20);

    assertFalse(checker.hasSpaceFor(2));
  }
}
