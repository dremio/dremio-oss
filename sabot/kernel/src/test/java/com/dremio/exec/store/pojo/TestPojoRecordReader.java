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

package com.dremio.exec.store.pojo;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.SampleMutator;
import com.dremio.test.AllocatorRule;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestPojoRecordReader {

  private BufferAllocator testAllocator;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-mutable-varchar-vector", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  @Test
  public void testPojoReaderBatched() throws ExecutionSetupException {

    TestPojo obj1 = new TestPojo(1, 2);
    TestPojo obj2 = new TestPojo(1, 2);
    List<TestPojo> list = Arrays.asList(obj1, obj2);

    try (PojoRecordReader<TestPojo> reader =
            new PojoRecordReader<>(TestPojo.class, list.iterator(), null, 1);
        SampleMutator mutator = new SampleMutator(testAllocator)) {
      reader.setup(mutator);
      int size = reader.next();
      Assert.assertTrue(size == 1);
      Assert.assertTrue(reader.next() == 1);
      Assert.assertTrue(reader.next() == 0);
    }
  }
}

class TestPojo {
  int testCol1, testCol2;

  public TestPojo(int testCol1, int testCol2) {
    this.testCol1 = testCol1;
    this.testCol2 = testCol2;
  }

  public int getTestCol1() {
    return testCol1;
  }

  public int getTestCol2() {
    return testCol2;
  }
}
