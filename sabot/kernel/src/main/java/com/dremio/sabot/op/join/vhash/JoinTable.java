/*
 * Copyright (C) 2017 Dremio Corporation
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

import io.netty.buffer.ArrowBuf;

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
}
