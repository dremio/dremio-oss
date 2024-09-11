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
package org.apache.arrow.vector.complex;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test class for {@link ListVectorHelper} */
class ListVectorHelperTest {

  private static BufferAllocator allocator;

  @BeforeAll
  public static void initAllocator() {
    allocator = new RootAllocator();
  }

  @AfterAll
  public static void closeAllocator() {
    allocator.close();
  }

  @Test
  public void testReallocValidityAndOffsetBuffers() {
    try (ListVector listVector = ListVector.empty("testListVector", allocator)) {
      listVector.addOrGetVector(FieldType.notNullable(new ArrowType.Int(32, true)));
      listVector.reAlloc();

      int origDataVectorCapacity = listVector.getDataVector().getValueCapacity();
      int expectedIndexToFit = listVector.getValueCapacity() + 10;

      ListVectorHelper.reallocValidityAndOffsetBuffers(listVector);

      assertThat(listVector.getDataVector().getValueCapacity())
          .withFailMessage("Data vector capacity should have not changed")
          .isEqualTo(origDataVectorCapacity);
      assertThat(listVector.getOffsetBufferValueCapacity())
          .isGreaterThanOrEqualTo(expectedIndexToFit);
      assertThat(LargeMemoryUtil.capAtMaxInt(listVector.getValidityBuffer().capacity() * 8L))
          .isGreaterThan(expectedIndexToFit);
    }
  }
}
