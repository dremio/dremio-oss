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

/**
 * MergeJoinOperator and its supporting classes.
 *
 * MergeJoinOperator implements the following merge-stage algorithm, in
 * the case of inner join:
 *
 * R as left table, S as right table
 *
 * while not done {
 *   while (r < s) { advance r, yield <r, null> if left or full join }
 *   while (r > s) { advance s, yield <null, s> if right or full join }
 *
 *   mark s // save start of “block”
 *
 *   while (r == s) {
 *     // Outer loop over r
 *     while (r == s) {
 *       // Inner loop over s
 *       yield <r, s>
 *       advance s
 *     }
 *
 *     advance r
 *     if (r == (s at marked position)) {
 *       reset s to mark
 *     } else {
 *       break
 *     }
 *   }
 * }
 *
 * On the high level, the algorithm is implemented by these classes:
 *
 * MergeJoinOperator: handle batching APIs
 *
 * MarkedAsyncIterator: keep marked positions across batches and store them internally
 *
 * MergeJoinComparator: handles the looping logic in the algorithm
 *
 * The looping logic is reflected in InternalState of MergeJoinComparator, while batching is
 * reflected in State of SortMergeJoinComparator
 *
 */
package com.dremio.sabot.op.join.merge;

