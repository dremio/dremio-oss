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
package com.dremio.sabot.op.join.nlje;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.VectorAccessible;

/** Generates matches for NLJE joins */
interface JoinMatcher extends AutoCloseable {

  /**
   * Output next set of values.
   *
   * @return Number of values output.
   */
  int output();

  void setup(
      LogicalExpression expr,
      ClassProducer classProducer,
      VectorAccessible probe,
      VectorAccessible build)
      throws Exception;

  /**
   * Read the new probe batch and determine how many matches there are.
   *
   * @param records Number of records in the batch.
   */
  void startNextProbe(int records);

  /**
   * Whether we have completely consumed the current batch and need another to produce more data.
   *
   * @return
   */
  boolean needNextInput();

  @Override
  void close() throws Exception;

  /**
   * Time spent copying.
   *
   * @return Time in nanoseconds
   */
  long getCopyNanos();

  /**
   * Time spent matching
   *
   * @return Time in nanoseconds
   */
  long getMatchNanos();

  /**
   * Get the number records that have been probed thus far.
   *
   * @return Number of probed records
   */
  long getProbeCount();
}
