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
package com.dremio.sabot.op.join.vhash;

import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;

public interface JoinTable extends AutoCloseable {
  public void insert(final ArrowBuf output, final int records);

  public void find(final ArrowBuf output, final int records);

  public int size();

  public int capacity();

  public int getRehashCount();

  public long getRehashTime(TimeUnit unit);

  public long getProbePivotTime(TimeUnit unit);

  public long getProbeFindTime(TimeUnit unit);

  public long getBuildPivotTime(TimeUnit unit);

  public long getInsertTime(TimeUnit unit);

  // Debugging methods

  /**
   * Start tracing this join table
   *
   * @param numRecords number of records in the current data batch
   * @return an autocloseable that, when closed, will release the resources held by the trace
   */
  AutoCloseable traceStart(int numRecords);

  /** Report the details of the trace */
  String traceReport();

  long getBuildHashComputationTime(TimeUnit unit);

  long getProbeHashComputationTime(TimeUnit unit);

  /**
   * Prepares bloomfilters for each probe target (field keys) in PartitionColFilters. Since this is
   * an optimisation, errors are not propagated to the consumer, instead, they marked as an empty
   * optional.
   *
   * @param partitionColFilters Previously created bloomfilters, one per probe target.
   * @param sizeDynamically Only used for EightByteInnerLeftProbeOff
   */
  void prepareBloomFilters(PartitionColFilters partitionColFilters, boolean sizeDynamically);

  /**
   * Prepares ValueListFilters for each probe target (and for each field for composite keys). Since
   * this is an optimisation, errors are not propagated to the consumer, instead they are ignored.
   *
   * @param nonPartitionColFilters Previously created value list builders, one list per probe
   *     target.
   */
  void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters);
}
