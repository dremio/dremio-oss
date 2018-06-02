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
package com.dremio.exec.work;

import java.sql.Timestamp;
import java.util.Iterator;

import com.dremio.sabot.task.TaskDescriptor;
import com.sun.xml.internal.xsom.impl.scd.Iterators;

public interface WorkStats {

  Iterator<FragmentInfo> getRunningFragments();

  /**
   * @return number of running fragments / max width per node
   */
  float getClusterLoad();


  Integer getCpuTrailingAverage(long id, int seconds);

  Integer getUserTrailingAverage(long id, int seconds);


  /**
   * Computes the reduction that should be applied to the default max_width_per_node option.<br>
   * As long as the cluster load is less than load.cut_off, no reduction will be applied.<br>
   * After that the reduction will be proportional to (cluster load x load.reduction)
   *
   * @return load reduction in the range [0, 1]
   */
  double getMaxWidthFactor();


  class FragmentInfo {
    public final String hostname;
    public final String queryId;
    public final int majorFragmentId;
    public final int minorFragmentId;
    public final Long memoryUsage;
    /**
     * The maximum number of input records across all Operators in fragment
     */
    public final Long rowsProcessed;
    public final Timestamp startTime;
    public final String blocks;

    public final int thread;
    public final String schedulerInfo;
    public final long sleeping;
    public final long blocked;

    public FragmentInfo(String hostname, String queryId, int majorFragmentId, int minorFragmentId, Long memoryUsage,
                        Long rowsProcessed, Timestamp startTime, String blocks, TaskDescriptor taskDescriptor) {
      this.hostname = hostname;
      this.queryId = queryId;
      this.majorFragmentId = majorFragmentId;
      this.minorFragmentId = minorFragmentId;
      this.memoryUsage = memoryUsage;
      this.rowsProcessed = rowsProcessed;
      this.startTime = startTime;
      this.blocks = blocks;
      this.thread = taskDescriptor.getThread();
      this.schedulerInfo = taskDescriptor.toString();
      this.sleeping = taskDescriptor.getSleepDuration();
      this.blocked = taskDescriptor.getTotalBlockedDuration();
    }
  }

  WorkStats NO_OP = new WorkStats(){

    @Override
    public Iterator<FragmentInfo> getRunningFragments() {
      return Iterators.empty();
    }

    @Override
    public float getClusterLoad() {
      return 1.0f;
    }

    @Override
    public Integer getCpuTrailingAverage(long id, int seconds) {
      return 0;
    }

    @Override
    public Integer getUserTrailingAverage(long id, int seconds) {
      return 0;
    }

    @Override
    public double getMaxWidthFactor() {
      return 1.0f;
    }};
}
