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
package com.dremio.common.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.service.Pointer;

import io.opentracing.mock.MockTracer;

/**
 * Tests for {@link ContextMigratingExecutorService}
 */
public class TestContextMigratingExecutorService {

  private final MockTracer tracer = new MockTracer();
  // single threaded pool to have a deterministic ordering of execution
  // and because one is enough to test migration.
  private final ExecutorService pool = new ContextMigratingExecutorService<>(
    new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(5)));

  @Before
  public void setup() {
  }

  private void blockingAsyncNoopWork() throws InterruptedException, ExecutionException {
    Future<?> future = pool.submit(() -> {});
    future.get();
  }

  @Test
  public void testNoSpanForWaitingOnCommandPoolIfNoActiveSpan() throws InterruptedException, ExecutionException {

    blockingAsyncNoopWork();

    Assert.assertEquals(0, tracer.finishedSpans().size());
  }

  @Test
  public void testContextWithCallable() throws Exception {
    final String testUser = "testUser1";

    Callable<String> callable = () -> RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    Future<String> future = RequestContext.empty()
      .with(UserContext.CTX_KEY, new UserContext(testUser))
      .call(() -> pool.submit(callable));
    Assert.assertEquals(testUser, future.get());
  }

  @Test
  public void testContextWithRunnable() throws Exception {
    final String testUser = "testUser2";
    final Pointer<String> foundUser = new Pointer<>();

    Runnable runnable = () -> foundUser.value = RequestContext.current().get(UserContext.CTX_KEY).getUserId();
    Future<?> future = RequestContext.empty()
      .with(UserContext.CTX_KEY, new UserContext(testUser))
      .call(() -> pool.submit(runnable));
    future.get();

    Assert.assertEquals(testUser, foundUser.value);
  }
}
