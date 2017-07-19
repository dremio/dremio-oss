/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.sort;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

import com.dremio.sabot.op.sort.external.SplayTree;
import com.dremio.sabot.op.sort.external.SplayTree.SplayIterator;

import io.netty.buffer.ArrowBuf;

public class TestSplayTree {


  @Test
  public void sortRandom(){
    final int[] values = new int[25000];
    final Random r = new Random();

    for(int i =0; i < values.length; i++){
      values[i] = r.nextInt();
    }

    final int comparisons = sortData(values);
  }

  @Test
  public void sortSame(){
    final int[] values = new int[25000];
    final Random r = new Random();

    for(int i =0; i < values.length; i++){
      values[i] = 47;
    }

    final int comparisons = sortData(values);
  }


  @Test
  public void sortEmpty(){
    final int[] values = new int[0];
    final int comparisons = sortData(values);
  }


  @Test
  public void sortAlreadyOrdered(){
    final int[] values = new int[25000];
    for(int i =0; i < values.length; i++){
      values[i] = i;
    }

    final int comparisons = sortData(values);
    assertTrue(comparisons < values.length);

  }

  @Test
  public void reverseSorted(){
    final int[] values = new int[25000];
    for(int i =0; i < values.length; i++){
      values[i] = values.length - i;
    }

    final int comparisons = sortData(values);
    assertTrue(comparisons < values.length);

  }

  private int sortData(int[] values){
    try(final SplayTreeImpl tree = new SplayTreeImpl(values);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowBuf data = allocator.buffer((values.length + 1) * SplayTree.NODE_SIZE )){

      data.setZero(0, data.capacity());
      tree.setData(data);

      for(int i = 0; i < values.length; i++){
        tree.put(i);
      }

      SplayIterator iter = tree.iterator();
      Integer previous = null;
      while(iter.hasNext()){
        int index = iter.next();
        if(previous != null){
          assertTrue(previous <= values[index]);
        }
      }

      return tree.getComparisonCount();
    }

  }

  private class SplayTreeImpl extends SplayTree implements AutoCloseable {

    private int[] values;
    private int comparisons;
    public SplayTreeImpl(int[] values) {
      this.values = values;
    }

    @Override
    public int compareValues(int leftVal, int rightVal) {
      comparisons++;
      return Integer.compare(values[leftVal], values[rightVal]);
    }

    public int getComparisonCount(){
      return comparisons;
    }

    @Override
    public void close() {
    }
  }
}
