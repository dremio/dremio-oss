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
package com.dremio.telemetry.api.metrics;

import java.util.concurrent.Callable;

import javax.annotation.CheckReturnValue;

import com.google.common.annotations.VisibleForTesting;

/**
 * A Metrics Logger/Instrumenter.
 * It's a helper class to allow logging basic counters and timers
 * using a simple API based on runnables and callables.
 * Operation instrumented by this class follow the following convention:
 * - [serviceName].[operationName].count for the number of times this operation has been called.
 * - [serviceName].[operationName].error for the number of errors.
 * - [serviceName].[operationName].time for the time it took to complete the operation.
 */
public final class MetricsInstrumenter {
  static final String ERROR_METRIC_SUFFIX = "error";
  static final String COUNT_METRIC_SUFFIX = "count";
  static final String TIME_METRIC_SUFFIX = "time";

  private final String serviceName;
  private final MetricsProvider provider;

  /**
   * Constructs a metrics instrumenter which service name is defined from the name of the passed in class.
   * @param serviceClass The class for which the service name is derived from.
   * @param <T> The class.
   */
  public <T> MetricsInstrumenter(Class<T> serviceClass) {
    this(serviceClass.getSimpleName(), new DefaultMetricsProvider());
  }

  /**
   * Constructs a metrics instrumenter which service name is passed in as parameter and which
   * counters and timers are created from the passed in provider.
   * @param serviceName The name of the service for this instrumenter.
   * @param provider The metrics provider used to create counters and timers used.
   */
  @VisibleForTesting
  MetricsInstrumenter(String serviceName, MetricsProvider provider) {
    this.serviceName = serviceName;
    this.provider = provider;
  }

  /**
   * Runs the provided runnable, logging count, errors and time.
   * @param operationName The name of the operation.
   * @param runnable The runnable to run.
   */
  public void log(String operationName, Runnable runnable) {
    instrument(operationName, runnable).run();
  }

  /**
   * Calls the provided Callable, logging count, errors and time.
   * Note: This method will wrap checked exceptions in a RuntimeException, since the callable
   * is being called as part of logging the metric. If you want to get the actual checked exception
   * you can use the instrument method that doesn't wrap checked exceptions.
   * @param operationName The name of the operation.
   * @param callable The callable to call.
   * @param <T> The return type of the callable.
   * @return The value returned by the execution of the callable.
   */
  public <T> T log(String operationName, Callable<T> callable) {
    try {
      return instrument(operationName, callable).call();
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Instruments the provided runnable returning another runnable
   * that when run logs count, errors and time.
   * @param operationName The name of the operation.
   * @param runnable The runnable to instrument.
   * @return An instrumented runnable.
   */
  @CheckReturnValue
  public Runnable instrument(String operationName, Runnable runnable) {
    return () -> {
      Counter counter = provider.counter(Metrics.join(serviceName, operationName, COUNT_METRIC_SUFFIX));
      Counter errorCounter = provider.counter(Metrics.join(serviceName, operationName, ERROR_METRIC_SUFFIX));
      Timer timer = provider.timer(Metrics.join(serviceName, operationName, TIME_METRIC_SUFFIX));
      try (Timer.TimerContext timerContext = timer.start()) {
        runnable.run();
      } catch (Throwable throwable) {
        errorCounter.increment();
        throw throwable;
      } finally {
        counter.increment();
      }
    };
  }

  /**
   * Instruments the provided callable returning another callable
   * that when calls logs count, errors and time.
   * @param operationName The name of the operation.
   * @param callable The callable to instrument.
   * @param <T> The return type of the callable.
   * @return An instrumented callable.
   */
  @CheckReturnValue
  public <T> Callable<T> instrument(String operationName, Callable<T> callable) {
    return () -> {
      Counter counter = provider.counter(Metrics.join(serviceName, operationName, COUNT_METRIC_SUFFIX));
      Counter errorCounter = provider.counter(Metrics.join(serviceName, operationName, ERROR_METRIC_SUFFIX));
      Timer timer = provider.timer(Metrics.join(serviceName, operationName, TIME_METRIC_SUFFIX));
      try(Timer.TimerContext timerContext = timer.start()) {
        return callable.call();
      } catch (Throwable throwable) {
        errorCounter.increment();
        throw throwable;
      } finally {
        counter.increment();
      }
    };
  }
}
