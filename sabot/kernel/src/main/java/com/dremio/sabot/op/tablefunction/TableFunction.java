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
package com.dremio.sabot.op.tablefunction;

import com.dremio.exec.record.VectorAccessible;

/**
 * Table function interface
 */
public interface TableFunction extends AutoCloseable{

  /**
   * Setup table function and return VectorAccessible with output schema
   * @param accessible VectorAccessible with input schema
   * @return VectorAccessible with output schema
   * @throws Exception
   */
  VectorAccessible setup(VectorAccessible accessible) throws Exception;

  /**
   * Start processing an input row
   * @param row to be processed
   */
  void startRow(int row) throws Exception;

  /**
   * Produce output records corresponding to current input row
   * @param startOutIndex start index in output vector
   * @param maxRecords maximum number of output records that can be produced
   * @return number of output records produced
   */
  int processRow(int startOutIndex, int maxRecords) throws Exception;

  /**
   * Stop processing current input row
   */
  void closeRow() throws Exception;
}
