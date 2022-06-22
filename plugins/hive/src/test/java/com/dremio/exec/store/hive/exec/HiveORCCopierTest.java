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
package com.dremio.exec.store.hive.exec;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.junit.Test;

import com.dremio.test.DremioTest;

public class HiveORCCopierTest extends DremioTest {

  private static int HIVE_BATCH_SIZE = 1024;
  private void getHiveBatch(ListColumnVector input, LongColumnVector child) {
    input.noNulls = false;

    input.childCount = 800;

    for(int i=0; i<HIVE_BATCH_SIZE; ++i) {
      child.vector[i] = 10 * i;
      input.offsets[i] = i;
      input.lengths[i] = 2;
    }
    for(int i=512; i<HIVE_BATCH_SIZE; ++i) {
      input.isNull[i] = true;
    }
  }

  @Test
  public void testListCopier() {
    LongColumnVector input1 = new LongColumnVector(HIVE_BATCH_SIZE);
    ListColumnVector input = new ListColumnVector(HIVE_BATCH_SIZE, input1);
    input.init();
    getHiveBatch(input, input1);

    HiveORCCopiers.ListCopier listCopier = new HiveORCCopiers.ListCopier((MultiValuedColumnVector)input);
    long childcountInFirstHalf = listCopier.countChildren(input.noNulls,
      input.lengths, 0, 512);
    long childcountInSecondHalf = listCopier.countChildren(input.noNulls,
      input.lengths, 512, 512);
    assertEquals(1024, childcountInFirstHalf);
    assertEquals(0, childcountInSecondHalf);
  }
}
