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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.dremio.common.util.TestTools;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.google.common.base.Preconditions;

public class TestFixedListVarcharVector extends DremioTest {
  private BufferAllocator testAllocator;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();
  @Rule
  public final TestRule timeoutRule = TestTools.getTimeoutRule(300, TimeUnit.SECONDS);

  @Before
  public void setupBeforeTest() {
    testAllocator = allocatorRule.newAllocator("test-fixedlist-varchar-vector", 0, Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  @Test
  public void TestBasic() {
    TestBasic(false, false, false);
    TestBasic(true, false, false);
    TestBasic(false, true, false);
    TestBasic(false, true, true);
    TestBasic(true, true, false);
    TestBasic(true, true, true);
  }

  private void TestBasic(boolean distinct, boolean orderby, boolean asc) {
    int batchSize = 100;

    FixedListVarcharVector flv = new FixedListVarcharVector("TestCompactionThreshold", testAllocator,
      batchSize, distinct, orderby, asc);

    int validitySize = FixedListVarcharVector.getValidityBufferSize(batchSize, orderby);
    int dataSize = FixedListVarcharVector.getDataBufferSize(batchSize, orderby);

    ArrowBuf validityBuf = testAllocator.buffer(validitySize);
    ArrowBuf dataBuf = testAllocator.buffer(dataSize);
    flv.loadBuffers(batchSize, dataBuf, validityBuf);
    validityBuf.getReferenceManager().release();
    dataBuf.getReferenceManager().release();

    try (ArrowBuf sampleDataBuf = testAllocator.buffer(512)) {
      final String insString1 = "aaaaa";
      final String insString2 = "bbbbbbbbbb";
      sampleDataBuf.setBytes(0, insString1.getBytes());
      sampleDataBuf.setBytes(insString1.length(), insString2.getBytes());

      flv.set(0, 0, insString1.length(), sampleDataBuf);
      flv.set(1, insString1.length(), insString2.length(), sampleDataBuf);

      byte[] str1 = flv.get(0);
      byte[] str2 = flv.get(1);
      Preconditions.checkState(Arrays.equals(str1, insString1.getBytes()));
      Preconditions.checkState(Arrays.equals(str2, insString2.getBytes()));
    } finally {
      flv.close();
    }
  }
}
