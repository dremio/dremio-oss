/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;

import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.DataRow;
import com.dremio.sabot.Fixtures.HeaderRow;
import com.dremio.sabot.Fixtures.Table;

class WindowGenerator {

  final static HeaderRow header = th("position_id", "sub", "salary");
  final static HeaderRow header4657 = th("position_id", "sub", "salary", "rn", "rnk");

  static DataRow[] generateInput(DataPar[] partitions) {

    // total number of rows
    int total = partitions[partitions.length - 1].cumulLength();
    DataRow[] rows = new DataRow[total];

    for (int id = 0; id < total; id++) {
      int p = 0;
      while (!partitions[p].isPartOf(id)) { // emp x is @ row x-1
        p++;
      }

      int sub = partitions[p].getSubId(id);
      int salary = partitions[p].salary(id);

      rows[id] = tr(p + 1, sub, salary);
    }

    return rows;
  }

  static Table generateOutput(DataPar[] partitions, boolean withOrderBy) {

    // total number of rows
    int total = partitions[partitions.length - 1].cumulLength();
    DataRow[] rows = new DataRow[total];

    int idx = 0;
    for (int p = 0; p < partitions.length; p++) {
      DataPar partition = partitions[p];
      for (int i = 0; i < partition.length; i++, idx++) {

        final int sub = partition.getSubId(idx);
        final long rank = 1 + partition.subRunningCount(sub) - partition.getSubSize(sub);
        final long denseRank = partition.getSubIndex(sub) + 1;
        final double cumeDist = (double) partition.subRunningCount(sub) / partition.length;
        final double percentRank = partition.length == 1 ? 0 : (double)(rank - 1)/(partition.length - 1);

        if (withOrderBy) {
          rows[idx] = tr(
            p+1,                                    // position_id
            sub,                                    // sub
            partition.salary(idx),                  // salary
            (long) partition.subRunningSum(sub),    // sum(salary)
            (long) partition.subRunningCount(sub),  // count(position_id)
            (long) (i + 1),                         // row_number()
            rank,                                   // rank()
            denseRank,                              // dense_rank()
            cumeDist,                               // cume_dist()
            percentRank                             // percent_rank()
          );
        } else {
          rows[idx] = tr(
            p+1,                                    // position_id
            sub,                                    // sub
            partition.salary(idx),                  // salary
            (long) partition.totalSalary(),         // sum(salary)
            (long) partition.length                 // count(position_id)
          );
        }
      }
    }

    if (withOrderBy) {
      return t(th("position_id", "sub", "salary", "sum", "count", "row_number", "rank", "dense_rank", "cume_dist", "percent_rank"), rows);
    } else {
      return t(th("position_id", "sub", "salary", "sum", "count"), rows);
    }
  }

  static DataRow[] generateOutput4657(DataPar[] partitions) {
    // we are just doing an order by on the partition column (position_id) and not a partition by
    // which means we treat partitions as frames:
    // we have a single partition: row_number is monotonically increasing
    // we have 2 frames one with 5 rows and the other one with all remaining rows

    // total number of rows
    int total = partitions[partitions.length - 1].cumulLength();
    DataRow[] rows = new DataRow[total];

    int idx = 0;
    long rank = 1;
    for (int p = 0; p < partitions.length; p++) {
      DataPar partition = partitions[p];
      for (int i = 0; i < partition.length; i++, idx++) {

        final int sub = partition.getSubId(idx);

        rows[idx] = tr(
          p+1,                            // position_id
          sub,                                    // sub
          partition.salary(idx),                  // salary
          (long) (idx + 1),                       // row_number()
          rank                                    // rank()
        );
      }
      rank += partition.length;
    }

    return rows;
  }

}
