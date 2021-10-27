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
package com.dremio.sabot.op.join.vhash.spill.list;

import static com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.getCarryAlongId;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecTest;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.CarryAlongId;
import com.dremio.sabot.op.join.vhash.spill.list.PageListMultimap.KeyAndCarryAlongId;
import com.dremio.sabot.op.join.vhash.spill.pool.PagePool;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;

public class TestPageListMultimap extends ExecTest {

  private PagePool pool;
  private PageListMultimap list;

  @Before
  public void before() {
    this.pool  = new PagePool(allocator, 16384, 0);
    this.list = new PageListMultimap(pool);
  }

  @After
  public void after() throws Exception {
    AutoCloseables.close(list, pool);
  }

  @Test
  public void basicList() {
    // remember, list items are in reverse order.
    list.insert(0, getCarryAlongId(1, 1));
    list.insert(1, getCarryAlongId(1, 2));
    list.insert(0, getCarryAlongId(1, 3));
    list.insert(1, getCarryAlongId(1, 4));
    list.insert(0, getCarryAlongId(1, 5));

    list.moveToRead();

    List<CarryAlongId> pos0 = pos(0);
    assertEquals(3, pos0.size());
    assertEquals(new CarryAlongId(1,1), pos0.get(2));
    assertEquals(new CarryAlongId(1,3), pos0.get(1));
    assertEquals(new CarryAlongId(1,5), pos0.get(0));

    List<CarryAlongId> pos1 = pos(1);
    assertEquals(2, pos1.size());
    assertEquals(new CarryAlongId(1,2), pos1.get(1));
    assertEquals(new CarryAlongId(1,4), pos1.get(0));

    // the full list should be returned in insert order.
    List<KeyAndCarryAlongId> fullList = list.getKeyAndCarryAlongIdStream().collect(Collectors.toList());
    assertEquals(new KeyAndCarryAlongId(0, new CarryAlongId(1, 1)), fullList.get(0));
    assertEquals(new KeyAndCarryAlongId(1, new CarryAlongId(1, 2)), fullList.get(1));
    assertEquals(new KeyAndCarryAlongId(0, new CarryAlongId(1, 3)), fullList.get(2));
    assertEquals(new KeyAndCarryAlongId(1, new CarryAlongId(1, 4)), fullList.get(3));
    assertEquals(new KeyAndCarryAlongId(0, new CarryAlongId(1, 5)), fullList.get(4));
  }

  @Test
  public void largeListViaCollection() throws Exception {

    // use a consistent random seed.
    Random r = new Random(1425657l);

    // generate batches of imaginary data until we've added.
    // For each batch, populate randomly among a set of possible keys

    final int totalElements = 100_000;
    final int totalKeys = 5_000;
    final int minBatchSize = 750;
    final int maxBatchSize = 5001;
    int elements = 0;
    LinkedListMultimap<Integer, CarryAlongId> objMap = LinkedListMultimap.create();
    List<KeyAndCarryAlongId> expectedFullList = new ArrayList<>();
    List<ArrowBuf> toRelease = new ArrayList<>();
    int batchId = 0;
    while (elements < totalElements) {
      int recordsInBatch = r.nextInt(maxBatchSize - minBatchSize) + minBatchSize;
      ArrowBuf keysBuffer = allocator.buffer(recordsInBatch * 4);
      toRelease.add(keysBuffer);
      int maxKey = 0;
      for (int recordIndex = 0; recordIndex < recordsInBatch; recordIndex++){
        int key = r.nextInt(totalKeys);
        maxKey = Math.max(maxKey, key);
        keysBuffer.setInt(recordIndex * 4, key);

        CarryAlongId carryAlongId = new CarryAlongId(batchId, recordIndex);
        objMap.put(key, carryAlongId);
        expectedFullList.add(new KeyAndCarryAlongId(key, carryAlongId));
      }

      list.insertCollection(keysBuffer, maxKey, batchId, 0, recordsInBatch);
      elements += recordsInBatch;
      batchId++;
    }
    assertEquals(expectedFullList, list.getKeyAndCarryAlongIdStream().collect(Collectors.toList()));

    list.moveToRead();

    // release offsets
    AutoCloseables.close(toRelease);

    // now verify results.
    for(Integer i : objMap.keySet()) {
      List<CarryAlongId> objList = Lists.reverse(objMap.get(i));
      List<CarryAlongId> pageList = pos(i);
      assertEquals(objList, pageList);
    }
  }

  @Test
  public void boundaryCases() {
    list.insert(0, getCarryAlongId(1, 65534));
    list.insert(0, getCarryAlongId(1, 65535));
    list.insert(1, getCarryAlongId(Integer.MAX_VALUE, 65534));
    list.insert(1, getCarryAlongId(Integer.MAX_VALUE, 65535));
    list.moveToRead();

    List<CarryAlongId> pos0 = pos(0);
    assertEquals(2, pos0.size());
    assertEquals(new CarryAlongId(1,65534), pos0.get(1));
    assertEquals(new CarryAlongId(1,65535), pos0.get(0));

    List<CarryAlongId> pos1 = pos(1);
    assertEquals(2, pos1.size());
    assertEquals(new CarryAlongId(Integer.MAX_VALUE,65534), pos1.get(1));
    assertEquals(new CarryAlongId(Integer.MAX_VALUE,65535), pos1.get(0));
  }

  @Test
  public void largeBatchNeg() {
    thrownException.expect(IllegalArgumentException.class);
    list.insert(0, getCarryAlongId(1, 65536));
  }

  @Test
  public void largeKeyNeg() {
    thrownException.expect(IllegalArgumentException.class);
    list.insert(Integer.MAX_VALUE, getCarryAlongId(1, 0));
  }

  @Test
  public void minusKeyNeg() {
    thrownException.expect(IllegalArgumentException.class);
    list.insert(-1, getCarryAlongId(1, 0));
  }

  private List<CarryAlongId> pos(int key) {
    return list.getStreamForKey(key).mapToObj(CarryAlongId::new).collect(Collectors.toList());
  }

}
