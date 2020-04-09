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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import com.dremio.common.tracing.TracingUtils;

/**
 * Responsible for ensuring the tracer active span is the same active span when the command is being run,
 * even if the command is run in a different thread.
 *
 * We don't implement the invoke methods since we don't use them anywhere within the dremio code base.
 */
public class ContextMigratingExecutorService<E extends ExecutorService> implements ExecutorService {

  public static final String WORK_OPERATION_NAME = "thread-pool-work";
  public static final String WAITING_OPERATION_NAME = "blocked-on-thread-pool";

  private final E delegate;
  private final Tracer tracer;

  public ContextMigratingExecutorService(E delegate, Tracer tracer) {
    this.delegate = delegate;
    this.tracer = tracer;
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(decorate(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(decorate(task), result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(decorate(task));
  }

  @Override
  public void execute(Runnable command) {
    delegate.execute(decorate(command));
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    throw new UnsupportedOperationException("ContextMigrator does not support invoke methods.");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("ContextMigrator does not support invoke methods.");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("ContextMigrator does not support invoke methods.");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {

    throw new UnsupportedOperationException("ContextMigrator does not support invoke methods.");
  }

  private Span makeWaitingSpan() {
    // We want to clearly see how long we spent waiting in the command pool.
    return TracingUtils.buildChildSpan(tracer, WAITING_OPERATION_NAME);
  }

  private <T> Callable<T> decorate(Callable<T> inner) {
    final Span parentSpan = tracer.activeSpan();
    final Span waitingSpan = makeWaitingSpan();

    // We only support plain callable types.
    return  () -> {
      final Span workSpan = atTaskStart(waitingSpan, parentSpan, tracer);
      try (Scope s = tracer.activateSpan(workSpan)) {
        return inner.call();
      } finally {
        workSpan.finish();
      }
    };
  }

  private static Span atTaskStart(Span waitingSpan, Span parentSpan, Tracer tracer) {
    final Thread thisThread = Thread.currentThread();
    waitingSpan.finish();
    return TracingUtils.childSpanBuilder(tracer, parentSpan, WORK_OPERATION_NAME,
      "thread-group", thisThread.getThreadGroup().getName(),
        "thread-name", thisThread.getName())
      .asChildOf(parentSpan)
      .start();

  }

  /**
   * Used to delegate comparison to the original runnable which has been validated as a comparable.
   */
  private static class ComparableRunnable implements Comparable<ComparableRunnable>, Runnable {
    private final Runnable comparableDelegate;
    private final Runnable work;

    ComparableRunnable(Runnable original, Runnable work) {
      Preconditions.checkArgument(original instanceof Comparable, "The delegate must be comparable");
      this.comparableDelegate = original;
      this.work = work;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(ComparableRunnable o) {
      return ((Comparable) comparableDelegate).compareTo(o.comparableDelegate);
    }

    @Override
    public void run() {
      work.run();
    }
  }

  private Runnable decorate(Runnable inner) {
    final Span parentSpan = tracer.activeSpan();
    final Span waitingSpan = makeWaitingSpan();
    final Function<Runnable, Runnable> factory;

    if (inner instanceof Comparable<?>) {
      factory = (runnable) -> new ComparableRunnable(inner, runnable);
    } else {
      factory = (runnable) -> runnable;
    }

    return factory.apply(() -> {
      final Span workSpan = atTaskStart(waitingSpan, parentSpan, tracer);
      try (Scope s = tracer.activateSpan(workSpan)) {
        inner.run();
      } finally {
        workSpan.finish();
      }
    });
  }

  public E getDelegate() {
    return delegate;
  }

  /**
   * We commonly wrap closeable thread pools. Create a decorator that works for closeable executor services.
   * @param <C> a closeableExecutorService
   */
  public static class ContextMigratingCloseableExecutorService<C extends AutoCloseable & ExecutorService>
    extends ContextMigratingExecutorService<C> implements CloseableExecutorService {

    private final C delegate;

    public ContextMigratingCloseableExecutorService(C delegate, Tracer tracer) {
      super(delegate, tracer);
      this.delegate = delegate;
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }
  }
}
