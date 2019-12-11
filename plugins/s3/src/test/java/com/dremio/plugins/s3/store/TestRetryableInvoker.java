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
package com.dremio.plugins.s3.store;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.SdkBaseException;

/**
 * Check that the retry mechanism works correctly.
 */
public class TestRetryableInvoker {
  @Test
  public void testNoRetry() throws Exception {
    testRetry(0);
  }

  @Test
  public void testOneRetry() throws Exception {
    testRetry(1);
  }

  @Test
  public void testMultipleRetry() throws Exception {
    testRetry(4);
  }

  private void testRetry(int numRetries) throws Exception {
    final S3AsyncByteReaderUsingSyncClient.RetryableInvoker invoker =
      new S3AsyncByteReaderUsingSyncClient.RetryableInvoker(numRetries);
    final AtomicInteger counter = new AtomicInteger(0);
    try {
      invoker.invoke(() -> {
        counter.incrementAndGet();
        throw new SdkBaseException("test exception");
      });
      Assert.fail();
    } catch (SdkBaseException e) {
      Assert.assertEquals(numRetries + 1, counter.get());
    }
  }
}
