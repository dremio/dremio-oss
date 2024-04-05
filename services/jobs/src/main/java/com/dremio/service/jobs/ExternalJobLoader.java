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
package com.dremio.service.jobs;

import com.dremio.common.DeferredException;
import java.util.concurrent.CountDownLatch;

/** External Job Loader */
public class ExternalJobLoader implements JobLoader {
  private final CountDownLatch completionLatch;
  private final DeferredException exception;

  public ExternalJobLoader(CountDownLatch completionLatch, DeferredException exception) {
    super();
    this.completionLatch = completionLatch;
    this.exception = exception;
  }

  @Override
  public RecordBatches load(int offset, int limit) {
    throw new UnsupportedOperationException(
        "Loading external job results isn't currently supported.");
  }

  @Override
  public void waitForCompletion() {
    try {
      completionLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      exception.addException(ex);
    }
  }

  @Override
  public String getJobResultsTable() {
    throw new UnsupportedOperationException("External job results are not stored.");
  }
}
