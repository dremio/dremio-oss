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
package com.dremio.exec.physical.base;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.store.schedule.CompleteWork;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;

/**
 * A GroupScan operator represents all data which will be scanned by a given physical plan. It is
 * the superset of all SubScans for the plan.
 */
public interface GroupScan<T extends CompleteWork> extends Scan {

  /**
   * columns list in GroupScan : 1) empty_column is for skipAll query. 2) NULL is interpreted as
   * ALL_COLUMNS. How to handle skipAll query is up to each storage plugin, with different policy in
   * corresponding RecordReader.
   */
  static final ImmutableList<SchemaPath> ALL_COLUMNS =
      ImmutableList.of(SchemaPath.getSimplePath("*"));

  static final long NO_COLUMN_STATS = -1;

  /**
   * Get splits associated with this scanning task.
   *
   * @return Splits
   */
  Iterator<T> getSplits(ExecutionNodeMap executionNodes);

  /**
   * For a given set of splits, get a specific operator.
   *
   * @param work
   * @return
   * @throws ExecutionSetupException
   */
  SubScan getSpecificScan(List<T> work) throws ExecutionSetupException;

  /**
   * Get the maximum number of parallel work units.
   *
   * @return
   */
  int getMaxParallelizationWidth();

  /**
   * At minimum, the GroupScan requires these many fragments to run. Currently, this is used in
   * {@link com.dremio.exec.planner.fragment.SimpleParallelizer}
   *
   * @return the minimum number of fragments that should run
   */
  int getMinParallelizationWidth();

  /**
   * Get the type of distribution expected by this operation.
   *
   * @return HARD, SOFT or NONE.
   */
  @JsonIgnore
  DistributionAffinity getDistributionAffinity();
}
