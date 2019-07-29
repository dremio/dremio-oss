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

import java.util.LinkedList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Stopwatch;

import io.airlift.tpch.RandomText;
import io.airlift.tpch.TextPool;

public class TestMutableVarcharVector {
  private BufferAllocator testAllocator;
  static final int TOTAL_STRINGS = 1024;
  static final int smallAvgSize = 15;
  static final int midAvgSize = 100;

  @Before
  public void setupBeforeTest() {
    testAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void cleanupAfterTest() throws Exception {
    testAllocator.close();
  }

  private LinkedList<String> GetRandomStringList(int N /*total strings*/, int avgSize /*average size of each string*/)
  {
    RandomText random = new RandomText(1500869201, TextPool.getDefaultTestPool(), avgSize, N);

    LinkedList<String> l1 = new LinkedList<String>();

    for (int i = 0; i < N; ++i) {
      l1.add(random.nextValue());
    }

    return l1;
  }

  /**
   * Insert 1000 strings of avg size 10
   * Update 1000 strings with avg size 100
   * Compact
   * Validate
   */
  @Test
  public void TestUpdateAfterInsert()
  {
    final double threshold = 1.0D;
    MutableVarcharVector m1 = new MutableVarcharVector("TestUpdateAfterInsert", testAllocator, threshold /*only force compact*/);
    try {
      LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
      LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

      //insert from l1
      for (int i = 0; i < TOTAL_STRINGS; ++i) {
        m1.setSafe(i, new Text(l1.get(i)));
      }
      m1.setValueCount(TOTAL_STRINGS);

      System.out.println("TestUpdateAfterInsert: threshold: " + threshold + " (only forcecompact)");
      System.out.println("TestUpdateAfterInsert: buffer capacity: " + m1.getByteCapacity());
      System.out.println("TestUpdateAfterInsert: Inserted " + TOTAL_STRINGS  + " strings with avg size: " + smallAvgSize);
      System.out.println("TestUpdateAfterInsert: Offset in buffer: " + m1.getCurrentOffset());

      //update from l2
      for (int i = 0; i < TOTAL_STRINGS; ++i) {
        m1.setSafe(i, new Text(l2.get(i)));
      }

      System.out.println("TestUpdateAfterInsert: Updated " + TOTAL_STRINGS  + " strings with avg size: " + midAvgSize);
      System.out.println("TestUpdateAfterInsert: Offset in buffer: " + m1.getCurrentOffset());

      //compact
      final int oldOffset = m1.getCurrentOffset();
      final Stopwatch ctcWatch = Stopwatch.createUnstarted();
      ctcWatch.start();
      m1.forceCompact();
      ctcWatch.stop();
      final int newOffset = m1.getCurrentOffset();

      System.out.println("TestUpdateAfterInsert: Post Compaction: Offset in buffer: " + newOffset);
      System.out.println("TestUpdateAfterInsert: Compaction time: " + ctcWatch.toString() + "\n");
      Assert.assertTrue(newOffset <= oldOffset);

      //match from l2
      for (int i = 0; i < TOTAL_STRINGS; ++i) {
        Assert.assertEquals(l2.get(i) /*expected*/, m1.getObject(i).toString() /*actual*/);
      }
    } finally {
      m1.close();
    }

  }

  /**
   * Insert at a position from l1 & immediately update it with a new value from l2,
   * repeat N times.
   */
  private void TestInterLeaved(MutableVarcharVector m1, LinkedList<String> l1, LinkedList<String> l2) {

    int expectedOffset = 0;
    for (int i = 0; i < TOTAL_STRINGS; ++i) {
      //insert at i from l1
      m1.setSafe(i, new Text(l1.get(i)));
      //update at i from l2
      m1.setSafe(i, new Text(l2.get(i)));

      expectedOffset += l2.get(i).length();
    }

    final int oldOffset = m1.getCurrentOffset();
    System.out.println("TestInterLeaved: buffer capacity: " + m1.getByteCapacity());
    System.out.println("TestInterLeaved: Offset in buffer: " + oldOffset);
    System.out.println("TestInterLeaved: Sum of all valid strings: " + expectedOffset);

    final Stopwatch ctcWatch = Stopwatch.createUnstarted();
    ctcWatch.start();
    m1.forceCompact();
    ctcWatch.stop();

    final int newOffset = m1.getCurrentOffset();
    System.out.println("TestInterLeaved: Post Compaction: Offset in buffer: " + newOffset);
    System.out.println("TestInterLeaved: Compaction time: " + ctcWatch.toString() + "\n");
    Assert.assertTrue(newOffset <= oldOffset);

    //post compaction, the offset should be just the sum of lengths of all valid data.
    Assert.assertEquals(expectedOffset, newOffset);

    //match from l2
    for (int i = 0; i < TOTAL_STRINGS; ++i) {
      Assert.assertEquals(l2.get(i) /*expected*/, m1.getObject(i).toString() /*actual*/);
    }
  }

  @Test
  public void TestInterleavedMidToSmall()
  {
    //insert the values from l1
    LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);
    //update values from l2
    LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);

    final double threshold = 1.0D;

    System.out.println("TestInterleavedMidToSmall: threshold: " + threshold +  " (only forcecompact)");
    MutableVarcharVector m1 = new MutableVarcharVector("TestInterleavedMidToSmall", testAllocator, threshold /*only force compact*/);
    try {
      TestInterLeaved(m1, l1, l2);
    } finally {
     m1.close();
    }
  }

  @Test
  public void TestInterleavedSmallToMid()
  {
    //insert the values from l1
    LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
    //update values from l2
    LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

    final double threshold = 1.0D;

    System.out.println("TestInterleavedSmallToMid: threshold: " + threshold + " (only forcecompact)");
    MutableVarcharVector m1 = new MutableVarcharVector("TestInterleavedSmallToMid", testAllocator, 1.0 /*only force compact*/);
    try {
      TestInterLeaved(m1, l1, l2);
    } finally {
      m1.close();
    }
  }

  @Test
  public void TestInterleavedWithLowThreshold()
  {
    //insert the values from l1
    LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
    //update values from l2
    LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

    final double threshold = 0.01D;

    System.out.println("TestInterleavedWithLowThreshold: " + threshold);
    System.out.println("Small -> Mid:");
    MutableVarcharVector m1 = new MutableVarcharVector("TestInterleavedWithLowThreshold", testAllocator, threshold);
    try {
      //small to mid
      TestInterLeaved(m1, l1, l2);
      m1.reset();

      //mid to small
      System.out.println("Mid -> Small:");
      TestInterLeaved(m1, l2, l1);
    } finally {
      m1.close();
    }
  }

  @Test
  public void TestInterleavedWithMidThreshold()
  {
    //insert the values from l1
    LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
    //update values from l2
    LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

    final double threshold = 0.5D;

    System.out.println("TestInterleavedWithMidThreshold: " + threshold);
    System.out.println("Small -> Mid:");
    MutableVarcharVector m1 = new MutableVarcharVector("TestInterleavedWithLowThreshold", testAllocator, threshold);
    try {
      //small to mid
      TestInterLeaved(m1, l1, l2);
      m1.reset();

      //mid to small
      System.out.println("Mid -> Small:");
      TestInterLeaved(m1, l2, l1);
    } finally {
      m1.close();
    }
  }

  @Test
  public void TestCompactionWithZeroThreshold()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCompactionThreshold", testAllocator, 0.7D);
    m1.setCompactionThreshold(0.0D);

    try {
      LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
      LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

      //insert
      for (int i = 0; i < TOTAL_STRINGS; ++i) {
        m1.setSafe(i, l1.get(i).getBytes());
      }

      //update each index.
      for (int i = 0; i < TOTAL_STRINGS; ++i) {
        final int oldOffset = m1.getCurrentOffset();
        //update -- this should always trigger compaction as threshold is 0
        m1.setSafe(i, l2.get(i).getBytes());
        final int newOffset = m1.getCurrentOffset();
        Assert.assertEquals(0, m1.getGarbageSizeInBytes());
        Assert.assertEquals((oldOffset - l1.get(i).length() + l2.get(i).length()), newOffset);
      }
    } finally {
      m1.close();
    }
  }

  @Test
  public void TestCompactionThreshold()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCompactionThreshold", testAllocator, 0.02D /* 2 percent */);

    try {
      m1.allocateNew(64 * 1024, 2);

      byte[] b1 = new byte[1024];
      byte[] b2 = new byte[512];

      //insert b1
      m1.setSafe(0, b1);
      //insert b2
      m1.setSafe(1, b1);

      Assert.assertEquals(0, m1.getGarbageSizeInBytes());

      //update
      m1.setSafe(1, b2);
      Assert.assertEquals(1024, m1.getGarbageSizeInBytes());

      //update - this should cause compaction.
      m1.setSafe(0, b2);
      Assert.assertEquals(0, m1.getGarbageSizeInBytes());

    } finally {
      m1.close();
    }
  }

  @Test
  public void TestNoCompaction()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCompactionThreshold", testAllocator, 1.0D /* never compact! */);
    try{

      LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
      for (int i = 0 ; i < TOTAL_STRINGS; ++i) {
        m1.setSafe(i, l1.get(i).getBytes());
      }

      final int waste = m1.getGarbageSizeInBytes();
      Assert.assertEquals(0, waste);

      final int oldOffset = m1.getCurrentOffset();
      final Stopwatch ctcWatch = Stopwatch.createUnstarted();
      m1.forceCompact();
      final int newOffset = m1.getCurrentOffset();

      //no change in data
      Assert.assertEquals(oldOffset, newOffset);
      Assert.assertEquals(0, waste);

    } finally {
      m1.close();
    }
  }

  @Test
  public void TestBasic()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCompactionThreshold", testAllocator, 1.0D /* never compact! */);
    try {
      final String insString1 = "aaaaa";
      final String insString2 = "bbbbbb";
      m1.setSafe(0, insString1.getBytes());
      m1.setSafe(1, insString2.getBytes());

      final String upd1 = "cccc";
      m1.setSafe(0, upd1.getBytes());

      final String upd2 = "ddd";
      m1.setSafe(1,upd2.getBytes());

      m1.forceCompact();

      Assert.assertEquals(upd1.length() + upd2.length(), m1.getCurrentOffset());

    } finally {
      m1.close();
    }
  }

  @Test
  public void TestCopyOut()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCopyOut", testAllocator, 1.0D /* never compact */);
    VarCharVector v1 = new VarCharVector("TestCopyOutVarchar", testAllocator);
    VarCharVector v2 = new VarCharVector("TestCopyOutVarcharPostCompaction", testAllocator);
    try {
      //insert the values from l1
      LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
      //update values from l2
      LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

      int expectedOffset = 0;
      int i = 0;
      final int firstIndex = 10; //beginning firstIndex records to be null

      //insert TOTAL_STRINGS nulls and values
      int startIdx = firstIndex;
      while (i < TOTAL_STRINGS) {
        //insert
        m1.setSafe(startIdx, new Text(l1.get(i)));
        //update
        m1.setSafe(startIdx, new Text(l2.get(i)));

        expectedOffset += l2.get(i).length();

        ++i;
        //create null values
        startIdx += 2;
      }

      //allocate with twice the index
      v1.allocateNew(m1.getUsedByteCapacity(), startIdx * 2);

      //copy records
      m1.copyToVarchar(v1, 0, startIdx * 3);

      //reset the value count back to actual strings copied
      v1.setValueCount(startIdx);

      //validate the records, skip null values
      i = 0;
      int j = firstIndex;
      while (i < TOTAL_STRINGS) {
        Assert.assertEquals(l2.get(i) /*expected*/, v1.getObject(j).toString() /*actual*/);
        ++i;
        j += 2; //skip nulls
      }

      //counters must match
      Assert.assertEquals(startIdx, j);
      Assert.assertEquals(TOTAL_STRINGS + firstIndex, v1.getNullCount());
    }
    finally {
      m1.close();
      v1.close();
    }

  }

  @Test
  public void TestCopyOutPostCompaction()
  {
    MutableVarcharVector m1 = new MutableVarcharVector("TestCopyOut", testAllocator, 1.0D /* never compact */);
    VarCharVector v2 = new VarCharVector("TestCopyOutVarcharPostCompaction", testAllocator);
    try {
      //insert the values from l1
      LinkedList<String> l1 = GetRandomStringList(TOTAL_STRINGS, smallAvgSize);
      //update values from l2
      LinkedList<String> l2 = GetRandomStringList(TOTAL_STRINGS, midAvgSize);

      int expectedOffset = 0;
      int i = 0;
      final int firstIndex = 10; //beginning firstIndex records to be null

      //insert TOTAL_STRINGS nulls and values
      int startIdx = firstIndex;
      while (i < TOTAL_STRINGS) {
        //insert
        m1.setSafe(startIdx, new Text(l1.get(i)));
        //update
        m1.setSafe(startIdx, new Text(l2.get(i)));

        expectedOffset += l2.get(i).length();

        ++i;
        //create null values
        startIdx += 2;
      }

      //Reduce garbage date
      m1.forceCompact();

      //allocate with twice the index
      v2.allocateNew(m1.getUsedByteCapacity(), startIdx * 2);

      //copy records
      m1.copyToVarchar(v2, 0, startIdx * 3);

      //reset the value count back to actual strings copied
      v2.setValueCount(startIdx);


      //verify that post compaction also data matches
      i = 0;
      int j = firstIndex;
      while (i < TOTAL_STRINGS) {
        Assert.assertEquals(l2.get(i) /*expected*/, v2.getObject(j).toString() /*actual*/);
        ++i;
        j += 2; //skip nulls
      }

      Assert.assertEquals(startIdx, j);
      Assert.assertEquals(TOTAL_STRINGS + firstIndex, v2.getNullCount());

    }
    finally {
      m1.close();
      v2.close();
    }

  }
}
