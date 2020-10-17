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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;

public interface JoinTable extends AutoCloseable {
  public void insert(final long outputAddr, final int records);
  public void find(final long outputAddr, final int records);
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
   * @param numRecords number of records in the current data batch
   * @return an autocloseable that, when closed, will release the resources held by the trace
   */
  public AutoCloseable traceStart(int numRecords);

  /**
   * Report the details of the trace
   */
  public String traceReport();
  public long getBuildHashComputationTime(TimeUnit unit);
  public long getProbeHashComputationTime(TimeUnit unit);

  /**
   * Prepares a bloomfilter from the selective field keys. Since this is an optimisation, errors are not propagated to
   * the consumer. Instead, they get an empty optional.
   * @param fieldNames
   * @param sizeDynamically Size the filter according to the number of entries in table.
   * @return
   */
  default Optional<BloomFilter> prepareBloomFilter(List<String> fieldNames, boolean sizeDynamically) {
    return Optional.empty();
  }

  /**
   * Returns distinct keys for a given field. In case of composite keys, this method can be used to get distinct values
   * for a given join field. Returns empty if number of distinct keys are more than max elements or if there is an
   * error while processing keys.
   *
   * Primarily used for Runtime Filtering at Joins
   *
   * @param fieldName
   * @param maxElements
   * @return
   */
  default Optional<ValueListFilter> prepareValueListFilter(String fieldName, int maxElements) {
    return Optional.empty();
  }
}
