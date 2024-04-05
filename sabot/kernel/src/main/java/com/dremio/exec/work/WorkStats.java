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
package com.dremio.exec.work;

import com.dremio.resource.GroupResourceInformation;
import com.dremio.sabot.task.TaskDescriptor;
import com.dremio.sabot.task.TaskPool;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;

public interface WorkStats {

  Iterator<FragmentInfo> getRunningFragments();

  default Iterable<TaskPool.ThreadInfo> getSlicingThreads() {
    return Collections.emptyList();
  }

  /**
   * @return number of running fragments / max width per node
   */
  float getClusterLoad();

  float getClusterLoad(GroupResourceInformation groupResourceInformation);

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

  double getMaxWidthFactor(GroupResourceInformation groupResourceInformation);

  /** sys.slicing_threads entry */
  class SlicingThreadInfo {

    public final String node_id;

    /** Sabot node infos */
    public final String hostname;

    public final int fabric_port;

    /** current Java thread name */
    public final String thread_name;

    /** slicing thread Id */
    public final int slicing_thread_id;

    /** OS thread Id */
    public final int os_thread_id;

    /** cpu id (core) */
    public final int cpu_id;

    public final int num_tasks;

    public final int num_staged_tasks;

    public final int num_requested_work;

    public SlicingThreadInfo(String hostName, int fabric_port, TaskPool.ThreadInfo info) {
      this.hostname = hostName;
      this.fabric_port = fabric_port;
      this.thread_name = info.threadName;
      this.slicing_thread_id = info.slicingThreadId;
      this.os_thread_id = info.osThreadId;
      this.cpu_id = info.cpuId;
      this.num_tasks = info.numTasks;
      this.num_staged_tasks = info.numStagedTasks;
      this.num_requested_work = info.numRequestedWork;
      this.node_id = hostName + ":" + fabric_port;
    }
  }

  class FragmentInfo {
    public final String node_id;
    public final String hostname;
    public final String job_id;
    public final int major_fragment_id;
    public final int minor_fragment_id;
    public final Long memory_usage;

    /** The maximum number of input records across all Operators in fragment */
    public final Long rows_processed;

    public final Timestamp start_time;
    public final String blocks;

    public final int thread;
    public final String scheduler_info;
    public final long sleeping;
    public final long blocked;
    public final long memory_grant;

    public FragmentInfo(
        String hostname,
        String job_id,
        int major_fragment_id,
        int minor_fragment_id,
        Long memory_usage,
        Long rows_processed,
        Timestamp start_time,
        String blocks,
        TaskDescriptor taskDescriptor,
        long fabric_port,
        long memory_grant) {
      this.hostname = hostname;
      this.job_id = job_id;
      this.major_fragment_id = major_fragment_id;
      this.minor_fragment_id = minor_fragment_id;
      this.memory_usage = memory_usage;
      this.rows_processed = rows_processed;
      this.start_time = start_time;
      this.blocks = blocks;
      this.thread = taskDescriptor.getThread();
      this.scheduler_info = taskDescriptor.toString();
      this.sleeping = taskDescriptor.getSleepDuration();
      this.blocked = taskDescriptor.getTotalBlockedDuration();
      this.node_id = hostname + ":" + fabric_port;
      this.memory_grant = memory_grant;
    }
  }

  WorkStats NO_OP =
      new WorkStats() {

        @Override
        public Iterator<FragmentInfo> getRunningFragments() {
          return Collections.emptyIterator();
        }

        @Override
        public float getClusterLoad(
            com.dremio.resource.GroupResourceInformation groupResourceInformation) {
          return getClusterLoad();
        }

        @Override
        public double getMaxWidthFactor(
            com.dremio.resource.GroupResourceInformation groupResourceInformation) {
          return getMaxWidthFactor();
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
        }
      };
}
