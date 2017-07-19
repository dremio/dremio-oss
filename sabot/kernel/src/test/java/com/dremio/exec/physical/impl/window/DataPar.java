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
package com.dremio.exec.physical.impl.window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Data partition used to generate tests for Window SqlOperatorImpl
 */
class DataPar {
  private DataPar previous;
  final int length;
  private final Integer[] subs;
  private final Integer[] subs_sizes;

  DataPar(int length, Integer[] subs, Integer[] sub_sizes) {
    this.length = length;
    this.subs = subs;
    this.subs_sizes = sub_sizes;
  }

  /**
   * @return total number of rows since first partition, this partition included
   */
  int cumulLength() {
    int prevLength = previous != null ? previous.cumulLength() : 0;
    return length + prevLength;
  }

  boolean isPartOf(int rowNumber) {
    int prevLength = previous != null ? previous.cumulLength() : 0;
    return rowNumber >= prevLength && rowNumber < cumulLength();
  }

  int getSubIndex(final int sub) {
    return Arrays.binarySearch(subs, sub);
  }

  int getSubSize(int sub) {
    if (sub != subs[subs.length - 1]) {
      return subs_sizes[getSubIndex(sub)];
    } else {
      //last sub has enough rows to reach partition length
      int size = length;
      for (int i = 0; i < subs.length - 1; i++) {
        size -= subs_sizes[i];
      }
      return size;
    }
  }

  /**
   * @return sub id of the sub that contains rowNumber
   */
  int getSubId(int rowNumber) {
    assert isPartOf(rowNumber) : "row "+rowNumber+" isn't part of this partition";

    int prevLength = previous != null ? previous.cumulLength() : 0;
    rowNumber -= prevLength; // row num from start of this partition

    for (int s : subs) {
      if (rowNumber < subRunningCount(s)) {
        return s;
      }
    }

    throw new RuntimeException("should never happen!");
  }

  int salary(int row) {
    return getSubId(row) + 10;
  }

  /**
   * @return running count of rows from first row of the partition to current sub, this sub included
   */
  int subRunningCount(int sub) {
    int count = 0;
    for (int s : subs) {
      count += getSubSize(s);
      if (s == sub) {
        break;
      }
    }
    return count;
  }

  /**
   * @return running sum of salaries from first row of the partition to current sub, this sub included
   */
  int subRunningSum(int sub) {
    int sum = 0;
    for (int s : subs) {
      sum += (s+10) * getSubSize(s);
      if (s == sub) {
        break;
      }
    }
    return sum;
  }

  /**
   * @return sum of salaries for all rows of the partition
   */
  int totalSalary() {
    return subRunningSum(subs[subs.length-1]);
  }

  private static class Builder {
    List<DataPar> partitions = new ArrayList<>();

    int cur_length;
    List<Integer> cur_subs = new ArrayList<>();
    List<Integer> cur_subs_size = new ArrayList<>();

    Builder partition(int length) {
      if (cur_length > 0) {
        addPartition();
      }

      cur_length = length;
      cur_subs.clear();
      cur_subs_size.clear();
      return this;
    }

    Builder sub(int subId) {
      return sub(subId, subId);
    }

    Builder sub(int subId, int num) {
      cur_subs.add(subId);
      cur_subs_size.add(num);
      return this;
    }

    void addPartition() {
      partitions.add(
              new DataPar(cur_length,
                      cur_subs.toArray(new Integer[cur_subs.size()]),
                      cur_subs_size.toArray(new Integer[cur_subs_size.size()])));
    }

    DataPar[] build() {
      if (cur_length > 0) {
        addPartition();
      }

      // set previous partitions
      for (int i = 1; i < partitions.size(); i++) {
        partitions.get(i).previous = partitions.get(i - 1);
      }

      return partitions.toArray(new DataPar[partitions.size()]);
    }
  }


  static DataPar[] dataB1P1() {
    // partition rows 20, subs [1, 2, 3, 4, 5, 6]
    return new Builder()
            .partition(20).sub(1).sub(2).sub(3).sub(4).sub(5).sub(6)
            .build();
  }

  static DataPar[] dataB1P2(boolean pby) {
    // partition rows 10, subs [1, 2, 3, 4]
    // partition rows 10, subs [4, 5, 6]
    if (pby) {
      return new Builder()
              .partition(10).sub(1).sub(2).sub(3).sub(4)
              .partition(10).sub(4).sub(5).sub(6)
              .build();
    } else {
      return new Builder()
              .partition(20).sub(1).sub(2).sub(3).sub(4, 8).sub(5).sub(6)
              .build();
    }
  }

  static DataPar[] dataB2P2(boolean pby) {
    // partition rows 20, subs [3, 5, 9]
    // partition rows 20, subs [9, 10]
    if (pby) {
      return new Builder()
              .partition(20).sub(3).sub(5).sub(9)
              .partition(20).sub(9).sub(10)
              .build();
    } else {
      return new Builder()
              .partition(40).sub(3).sub(5).sub(9, 12 + 9).sub(10)
              .build();
    }
  }

  static DataPar[] dataB2P4(boolean pby) {
    // partition rows 5, subs [1, 2, 3]
    // partition rows 10, subs [3, 4, 5]
    // partition rows 15, subs [5, 6, 7]
    // partition rows 10, subs [7, 8]
    if (pby) {
      return new Builder()
              .partition(5).sub(1).sub(2).sub(3)
              .partition(10).sub(3).sub(4).sub(5)
              .partition(15).sub(5).sub(6).sub(7)
              .partition(10).sub(7).sub(8)
              .build();
    } else {
      return new Builder()
              .partition(40).sub(1).sub(2).sub(3, 5).sub(4).sub(5, 8).sub(6).sub(7, 11).sub(8)
              .build();
    }
  }

  static DataPar[] dataB3P2(boolean pby) {
    // partition rows 5, subs [1, 2, 3]
    // partition rows 55, subs [4, 5, 7, 8, 9, 10, 11, 12]
    if (pby) {
      return new Builder()
              .partition(5).sub(1).sub(2).sub(3)
              .partition(55).sub(4).sub(5).sub(7).sub(8).sub(9).sub(10).sub(11).sub(12)
              .build();
    } else {
      return new Builder()
              .partition(60).sub(1).sub(2).sub(3, 2).sub(4).sub(5).sub(7).sub(8).sub(9).sub(10).sub(11).sub(12)
              .build();
    }
  }

  static DataPar[] dataB4P4(boolean pby) {
    // partition rows 10, subs [1, 2, 3]
    // partition rows 30, subs [3, 4, 5, 6, 7, 8]
    // partition rows 20, subs [8, 9, 10]
    // partition rows 20, subs [10, 11]
    if (pby) {
      return new Builder()
              .partition(10).sub(1).sub(2).sub(3)
              .partition(30).sub(3).sub(4).sub(5).sub(6).sub(7).sub(8)
              .partition(20).sub(8).sub(9).sub(10)
              .partition(20).sub(10).sub(11)
              .build();
    } else {
      return new Builder()
              .partition(80).sub(1).sub(2).sub(3, 10)
              .sub(4).sub(5).sub(6).sub(7).sub(8, 13)
              .sub(9).sub(10, 13).sub(11, 10)
              .build();
    }
  }

}
