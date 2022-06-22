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
package com.dremio.common.expression;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.dremio.test.DremioTest;

public class PathSegmentTest extends DremioTest {
  protected PathSegment makeArraySegment(final int len, final PathSegment tail) {
    PathSegment node = tail;
    for (int i = 0; i < len; i++) {
      node = new PathSegment.ArraySegment(node);
    }
    return node;
  }

  @Test
  public void testIfMultiLevelCloneWorks() {
    final int levels = 10;
    final PathSegment segment = new PathSegment.NameSegment("test", makeArraySegment(levels, null));
    final PathSegment clone = segment.clone();
    assertEquals("result of clone & original segments must be identical", segment, clone);

    final PathSegment tail = new PathSegment.NameSegment("tail");
    final PathSegment newSegment = new PathSegment.NameSegment("test", makeArraySegment(levels, tail));
    final PathSegment newClone = segment.cloneWithNewChild(tail);
    assertEquals("result of cloneWithChild & original segment must be identical", newSegment, newClone);
  }
}
