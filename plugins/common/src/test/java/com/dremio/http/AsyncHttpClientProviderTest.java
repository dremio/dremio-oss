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
package com.dremio.http;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Tests for {@link AsyncHttpClientProvider} */
public class AsyncHttpClientProviderTest {

  @Test
  public void testSameInstance() throws InterruptedException {
    ExecutorService ex = Executors.newFixedThreadPool(10);
    final Set<Integer> objectIdentities = new ConcurrentSkipListSet<>();
    CountDownLatch latch = new CountDownLatch(100);
    for (int i = 0; i < 100; i++) {
      ex.execute(
          () -> {
            objectIdentities.add(System.identityHashCode(AsyncHttpClientProvider.getInstance()));
            latch.countDown();
          });
    }
    latch.await(5, TimeUnit.MINUTES);
    assertEquals(1, objectIdentities.size()); // same object returned every time
    ex.shutdownNow();
  }
}
