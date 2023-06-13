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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.noop.NoopSpan;

/**
 * Tests for {@link ContextMigratingExecutorService}
 */
public class TestContextMigratingExecutorService {

  private static String TEST_WORK_NAME = "span-test-work";

  private MockTracer tracer = new MockTracer();
  // single threaded pool to have a deterministic ordering of execution
  // and because one is enough to test migration.
  private ExecutorService pool = new ContextMigratingExecutorService<>(
    new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(5)),
    tracer);

  private MockSpan testSpan;

  @Before
  public void setup() {
    tracer.reset();
    // test span is not activated.
    testSpan = tracer.buildSpan("test-parent-span").start();
  }

  private Future<Span> submitActiveSpanCopyWork() {
    return pool.submit(() -> tracer.activeSpan());
  }

  private void assertChildSpan(Future<Span> child) throws InterruptedException, ExecutionException {
   assertChildSpan(child.get());
  }

  private void assertChildSpan(Span child) {
    MockSpan actual = ((MockSpan) child);
    Assert.assertEquals(testSpan.context().spanId(), actual.parentId());
    // The waiting span should be the first finished span.
    // Then the span for the command pool work.
    Assert.assertEquals(tracer.finishedSpans().get(1), actual);
    Assert.assertEquals(ContextMigratingExecutorService.WORK_OPERATION_NAME, actual.operationName());
  }

  @Test
  public void testChildSpanForWorkCreatedOnCommandPoolThread() throws InterruptedException, ExecutionException {

    final Future<Span> activeSpanCheck;

    try (Scope s = tracer.activateSpan(testSpan)) {
      activeSpanCheck = submitActiveSpanCopyWork();
    }

    assertChildSpan(activeSpanCheck);
  }

  @Test
  public void testNoActiveSpanResultsInNoopSpan() throws InterruptedException, ExecutionException {
    final Future<Span> activeSpanCheck;

    // Run a command with a parent to "dirty" the thread.
    try (Scope s = tracer.activateSpan(testSpan)) {
      submitActiveSpanCopyWork();
    }

    activeSpanCheck = submitActiveSpanCopyWork();

    Assert.assertEquals(NoopSpan.INSTANCE, activeSpanCheck.get());
  }

  private void blockingAsyncNoopWork() throws InterruptedException, ExecutionException {
    Future<?> future = pool.submit(() -> {});
    future.get();
  }

  @Test
  public void testSpanForWaitingOnCommandPool() throws InterruptedException, ExecutionException {

    try (Scope s = tracer.activateSpan(testSpan)) {
      // Essentially noop - we are concerned with seeing the span for waiting on the command pool.
      blockingAsyncNoopWork();
    }

    MockSpan waiting = tracer.finishedSpans().get(0);
    Assert.assertEquals(
      ContextMigratingExecutorService.WAITING_OPERATION_NAME,
      waiting.operationName());
    Assert.assertEquals(testSpan.context().spanId(), waiting.parentId());
  }

  @Test
  public void testNoSpanForWaitingOnCommandPoolIfNoActiveSpan() throws InterruptedException, ExecutionException {

    blockingAsyncNoopWork();

    Assert.assertEquals(0, tracer.finishedSpans().size());
  }

  @Test
  public void testRunnableDecoration() throws InterruptedException, ExecutionException {
    Span[] runnableSpan = new Span[1];

    try (Scope s = tracer.activateSpan(testSpan)) {
      Future<?> future = pool.submit(() -> {
        runnableSpan[0] = tracer.activeSpan();
      });
      future.get();
    }

    assertChildSpan(runnableSpan[0]);
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
