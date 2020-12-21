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
package com.dremio.exec.record;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents a subset of data in {@link RecordBatchData}.
 */
public final class RecordBatchHolder implements AutoCloseable {

  private final RecordBatchData data;
  private final int start;
  private final int end;

  private RecordBatchHolder(final RecordBatchData data, final int start, final int end) {
    this.data = data;
    this.start = start;
    this.end = end;
  }

  /**
   * Create a new {@link RecordBatchHolder}
   * @param data underlying {@link RecordBatchData}
   * @param start starting index where the interested data starts.
   * @param end ending index (exclusive) where interested data ends.
   * @return
   */
  public static RecordBatchHolder newRecordBatchHolder(final RecordBatchData data, final int start, final int end) {
    checkArgument(start >= 0,
        "Invalid start index (%s) in RecordBatchData (of size (%s))", start, data.getRecordCount());
    checkArgument(end <= data.getRecordCount(),
        "Invalid end index (%s) in RecordBatchData (of size (%s))", end, data.getRecordCount());
    checkArgument(start <= end,
        "Invalid range indices. Start (%s), End (%s), Batch size (%s)", start, end, data.getRecordCount());

    return new RecordBatchHolder(data, start, end);
  }

  /**
   * Get the underlying {@link RecordBatchData}
   * @return
   */
  public RecordBatchData getData() {
    return data;
  }


  /**
   * Get the start index in underlying index where the interested data starts.
   * @return
   */
  public int getStart() {
    return start;
  }

  /**
   * Get the last index (exclusive) where the interested data ends.
   * @return
   */
  public int getEnd() {
    return end;
  }

  /**
   * Total number of records in subset.
   * @return
   */
  public int size() {
    return end - start;
  }

  @Override
  public void close() throws Exception {
    data.close();
  }
}
