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

import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Tests {@link AbstractVector#reAlloc()} */
public class TestSimpleIntVectorRealloc extends DremioTest {

  private BufferAllocator testAllocator;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-clear-realloc", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  @Test
  public void testNoCapacityChangeWithClearReallocCycles() throws Exception {
    try (final SimpleIntVector vector = new SimpleIntVector("simpleint", testAllocator)) {
      vector.allocateNew();
      vector.setSafe(0, 1);
      vector.setSafe(1, 1);
      vector.setSafe(2, 1);
      vector.setSafe(3, 1);

      int savedValueCapacity = vector.getValueCapacity();

      for (int i = 0; i < 1024; ++i) {
        vector.clear();
        vector.setSafe(0, 1); // this reallocs
      }

      // reallocs above should not increase the capacity
      Assert.assertEquals(vector.getValueCapacity(), savedValueCapacity);
    }
  }
}
