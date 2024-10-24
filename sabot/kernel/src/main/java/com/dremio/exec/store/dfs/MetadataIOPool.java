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
package com.dremio.exec.store.dfs;

import com.dremio.common.VM;
import com.dremio.common.concurrent.ContextAwareCompletableFuture;
import com.dremio.common.utils.PathUtils;
import com.dremio.config.DremioConfig;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.context.RequestContext;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetadataIOPool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MetadataIOPool.class);
  private static final String POOL_SIZE = "services.coordinator.metadata-io-pool.size";
  private static final String SERVICE_NAME = "dremio-metadata-io-pool";
  private static final String PREFIX = "dremio.metadata.io.";
  private static final String TASK_NAME = PREFIX + "task.name";
  private static final String ENTITY_PATH = PREFIX + "task.entity_path";
  private static final String CAPACITY_EXCEEDED = PREFIX + "executor.capacity.exceeded";

  private final Executor executor;
  private ThreadPoolExecutor mainPool;
  private Meter.MeterProvider<Counter> overflowTaskCounter;
  private Meter.MeterProvider<Timer> taskTimer;

  private MetadataIOPool(DremioConfig config) {
    this(config == null ? VM.availableProcessors() * 4 : getPoolSize(config));
  }

  private MetadataIOPool(int size) {
    executor = createNewPool(size);
  }

  private static int getPoolSize(final DremioConfig config) {
    int size = config.getInt(POOL_SIZE);
    return size > 0 ? VM.availableProcessors() * size : 0;
  }

  public <T> CompletionStage<T> execute(@Nonnull MetadataTask<T> task) {
    ContextAwareCompletableFuture<T> completion = new ContextAwareCompletableFuture<>();
    executor.execute(new MetadataTaskRunnable<>(task, completion));
    return completion;
  }

  @Override
  public void close() throws Exception {
    mainPool.shutdownNow();
  }

  private Executor createNewPool(int poolSize) {
    if (poolSize == 0) {
      return Runnable::run;
    }

    mainPool =
        new ThreadPoolExecutor(
            0,
            poolSize,
            60,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new ThreadFactoryBuilder()
                .setThreadFactory(MetadataIOPoolThread::new)
                .setNameFormat(SERVICE_NAME + "-%d")
                .build(),
            new MaxCapacityHandler());

    // Instrument the main pool with metrics collection
    new ExecutorServiceMetrics(mainPool, SERVICE_NAME, PREFIX, null).bindTo(Metrics.globalRegistry);

    overflowTaskCounter =
        Counter.builder(PREFIX + "executor.overflow")
            .description(
                "The approximate total number of tasks that have executed on the caller thread "
                    + "because the pool was at capacity")
            .baseUnit(BaseUnits.TASKS)
            .withRegistry(Metrics.globalRegistry);

    this.taskTimer =
        Timer.builder(PREFIX + "executor.execution")
            .description("Records task execution time")
            .publishPercentileHistogram()
            .withRegistry(Metrics.globalRegistry);

    return runnable -> {
      // If a task is submitted from a thread owned by this pool, simply run on the current thread.
      // This ensures we are not unnecessarily invoking the MaxCapacityHandler.
      if (Thread.currentThread() instanceof MetadataIOPoolThread) {
        runnable.run();
      }
      mainPool.execute(runnable);
    };
  }

  public static final class Factory {

    public static final Factory INSTANCE = new Factory();

    public MetadataIOPool newPool(DremioConfig config) {
      return new MetadataIOPool(config);
    }

    public MetadataIOPool newPool(int size) {
      return new MetadataIOPool(size);
    }
  }

  public static final class MetadataTask<T> {

    private final String taskName;
    private final EntityPath entityPath;
    private final Supplier<T> taskAction;

    private T result;

    public MetadataTask(String taskName, EntityPath entityPath, Supplier<T> taskAction) {
      this.taskName = taskName;
      this.entityPath = entityPath;
      this.taskAction = taskAction;
    }

    public String getTaskName() {
      return taskName;
    }

    public EntityPath getEntityPath() {
      return entityPath;
    }

    public T getResult() {
      if (result == null) {
        result = taskAction.get();
      }
      return result;
    }
  }

  private final class MetadataTaskRunnable<T> implements Runnable {

    private final MetadataTask<T> task;
    private final CompletableFuture<T> completionHandle;
    private final RequestContext requestContext;
    private final Context traceContext;

    private MetadataTaskRunnable(MetadataTask<T> task, CompletableFuture<T> completionHandle) {
      this(task, completionHandle, RequestContext.current(), Context.current());
    }

    private MetadataTaskRunnable(
        MetadataTask<T> task,
        CompletableFuture<T> completionHandle,
        RequestContext requestContext,
        Context traceContext) {
      this.task = task;
      this.completionHandle = completionHandle;
      this.requestContext = requestContext;
      this.traceContext = traceContext;
    }

    public MetadataTask<T> getTask() {
      return task;
    }

    @Override
    public void run() {
      requestContext.run(traceContext.wrap(this::runTask));
    }

    @WithSpan(value = "metadata-io-task")
    private void runTask() {
      String taskName = task.getTaskName();
      String entityPath = PathUtils.constructFullPath(task.getEntityPath().getComponents());
      Span.current().setAttribute(TASK_NAME, taskName);
      Span.current().setAttribute(ENTITY_PATH, entityPath);
      Timer.Sample executionSample = Timer.start(Metrics.globalRegistry);
      try {
        completionHandle.complete(task.getResult());
      } catch (Throwable ex) {
        completionHandle.completeExceptionally(ex);
      } finally {
        if (taskTimer != null) {
          executionSample.stop(taskTimer.withTags());
        }
      }
    }
  }

  /**
   * This is the same as a {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} for when
   * there are no available threads in the pool, but with some added telemetry.
   */
  private final class MaxCapacityHandler implements RejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
      if (!executor.isShutdown()) {

        Span.current().setAttribute(CAPACITY_EXCEEDED, true);

        task.run();
        overflowTaskCounter.withTags().increment();

        if (task instanceof MetadataTaskRunnable) {
          MetadataTask<?> metadataTask = ((MetadataTaskRunnable<?>) task).getTask();
          String taskName = metadataTask.getTaskName();
          EntityPath entityPath = metadataTask.getEntityPath();
          String pathStr = PathUtils.constructFullPath(entityPath.getComponents());

          logger.info(
              "{} is at max capacity. Running task {}:{} on caller thread",
              SERVICE_NAME,
              taskName,
              pathStr);
        }
      }
    }
  }

  /** Serves as a tag for threads owned by this pool */
  private static final class MetadataIOPoolThread extends Thread {

    public MetadataIOPoolThread(Runnable runnable) {
      super(runnable);
    }
  }
}
