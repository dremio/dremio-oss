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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import com.dremio.sabot.op.common.ht2.ResizeListenerNoSpill;

/**
 * Interface for implementing a measure. Maintains an array of workspace and/or
 * output vectors as well as a refrence to the input vector.
 */
public interface AccumulatorNoSpill extends ResizeListenerNoSpill, AutoCloseable {

  /**
   * Accumulate the data that is specified at the provided offset vector. The
   * offset vector describes which local mapping each of the <count> records
   * should be addressed.
   *
   * @param offsetAddr
   * @param count
   */
  void accumulate(long offsetAddr, int count);

  /**
   * Output the data for the provided the batch index to the output vectors.
   * @param batchIndex
   */
  void output(int batchIndex);
}
