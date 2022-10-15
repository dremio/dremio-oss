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
package com.dremio.sabot.op.join.vhash.spill.partition;

import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;

import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.VariableBlockVector;
import com.dremio.sabot.op.join.vhash.NonPartitionColFilters;
import com.dremio.sabot.op.join.vhash.PartitionColFilters;

public interface JoinTable extends AutoCloseable {
  /**
   * Compute hash for given records
   *
   * @param records number of records
   * @param keyFixed pivoted fixed buffer
   * @param keyVar pivoted variable buffer
   * @param seed seed to use when computing the hash
   * @param hashOut8B buffer for computed hash values
   */
  void hashPivoted(int records, ArrowBuf keyFixed, ArrowBuf keyVar, long seed, ArrowBuf hashOut8B);

  /**
   * Insert pivoted keys into the hash-table.
   *
   * @param sv2 ordinals of the records to insert
   * @param shift to be decremented to get to the corresponding record in the pivoted block
   * @param records number of records
   * @param tableHash4B hash-values of the records
   * @param fixed fixed block
   * @param variable var block
   * @param output ordinals for the inserted records in the hash table
   */
  public int insertPivoted(ArrowBuf sv2, int pivotShift, int records,
                           ArrowBuf tableHash4B, FixedBlockVector fixed, VariableBlockVector variable,
                           ArrowBuf output);

  /**
   * Find records corresponding to the pivoted keys in the hash-table.
   *
   * @param sv2 ordinals of the records to insert
   * @param records number of records
   * @param tableHash4B hash-values of the records
   * @param fixed fixed block
   * @param variable var block
   * @param output ordinals for the inserted records in the hash table
   */
  public void findPivoted(ArrowBuf sv2, int pivotShift, int records,
                          ArrowBuf tableHash4B, FixedBlockVector fixed, VariableBlockVector variable,
                          ArrowBuf output);

  int getMaxOrdinal();
  int size();
  int capacity();
  int getRehashCount();
  long getRehashTime(TimeUnit unit);
  long getProbeFindTime(TimeUnit unit);
  long getInsertTime(TimeUnit unit);

  /**
   * Copies the referenced keys to the provided output.
   * @param keyOffsetBuf
   * @param count
   * @param keyFixed
   * @param keyVar
   */
  public void copyKeysToBuffer(final ArrowBuf keyOffsetBuf, final int count, final ArrowBuf keyFixed, final ArrowBuf keyVar);

  /**
   * Provides the total var key length for the referenced keys
   * @param keyOffsetBuf
   * @param count
   * @return
   */
  public int getCumulativeVarKeyLength(final ArrowBuf keyOffsetBuf, final int count);

  /**
   * Get the var lengths for the referenced keys
   * @param keyOffsetBuf
   * @param count
   * @param outBuf
   * @return
   */
  public void getVarKeyLengths(final ArrowBuf keyOffsetBuf, final int count, final ArrowBuf outBuf);

  // Debugging methods

  /**
   * Start tracing this join table
   * @param numRecords number of records in the current data batch
   * @return an autocloseable that, when closed, will release the resources held by the trace
   */
  AutoCloseable traceStart(int numRecords);

  /**
   * Report the details of the trace
   */
  String traceReport();

  /**
   * Prepares bloomfilters for each probe target (field keys) in PartitionColFilters.
   * Since this is an optimisation, errors are not propagated to the consumer,
   * instead, they marked as an empty optional.
   *
   * @param partitionColFilters Previously created bloomfilters, one per probe target.
   */
  void prepareBloomFilters(PartitionColFilters partitionColFilters);

  /**
   * Prepares ValueListFilters for each probe target (and for each field for composite keys).
   * Since this is an optimisation, errors are not propagated to the consumer, instead they
   * are ignored.
   *
   * @param nonPartitionColFilters Previously created value list builders, one list per probe target.
   */
  void prepareValueListFilters(NonPartitionColFilters nonPartitionColFilters);
}
