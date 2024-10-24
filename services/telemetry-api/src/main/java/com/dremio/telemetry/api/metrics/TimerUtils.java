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

import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_KEY;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_NONE;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_ERROR;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_SUCCESS;
import static com.dremio.telemetry.api.metrics.MeterProviders.newTimerResourceSample;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.function.Supplier;

/** Utility class for timing operations. */
public final class TimerUtils {
  /**
   * Create a new Timer.ResourceSample with the given name, description, and tags. When in use, the
   * Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample timed(String name, String description, String... tags) {
    return newTimerResourceSample(name, description == null ? "" : description, Tags.of(tags));
  }

  /**
   * Create a new Timer.ResourceSample with the given name, description, and tags iterable. When in
   * use, the Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample timed(String name, String description, Iterable<Tag> tags) {
    return newTimerResourceSample(name, description == null ? "" : description, Tags.of(tags));
  }

  /**
   * Create a new Timer.ResourceSample with the given name, description, minimum expected value for
   * the Timer, and tags. When in use, the Timer will be registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry and will publish percentile histogram.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param minMs the minimum expected value for the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample timedHistogram(
      String name, String description, Duration minMs, String... tags) {
    return timed(name, description, tags).publishPercentileHistogram().minimumExpectedValue(minMs);
  }

  /**
   * Create a new Timer.ResourceSample with the given name, description, and tags iterable. When in
   * use, the Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry and
   * will publish percentile histogram.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample timedHistogram(
      String name, String description, Iterable<Tag> tags) {
    return timed(name, description, tags).publishPercentileHistogram();
  }

  /**
   * Create a new Timer.ResourceSample with the given name, description, and tags. When in use, the
   * Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry and will
   * publish percentile histogram.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample timedHistogram(
      String name, String description, String... tags) {
    return timed(name, description, tags).publishPercentileHistogram();
  }

  /**
   * Times the given operation and returns its result. Applies the outcome tag indicating success or
   * error of the operation. If the operation throws an exception, the exception tag is also applied
   * and the exception is re-thrown.
   *
   * @param sample the Timer.ResourceSample to use for timing
   * @param operation the operation to time
   * @param <V> the type of the operation's result
   * @return the result of the operation
   */
  public static <V> V timedOperation(Timer.ResourceSample sample, Supplier<V> operation) {
    try {
      try {
        final V ret = operation.get();
        sample.tags(Tags.of(TAG_OUTCOME_SUCCESS, TAG_EXCEPTION_NONE));
        return ret;
      } catch (final Exception ex) {
        sample.tags(
            Tags.of(TAG_OUTCOME_ERROR, Tag.of(TAG_EXCEPTION_KEY, ex.getClass().getSimpleName())));
        throw ex;
      }
    } finally {
      sample.close(); // after moving to JAVA 11 this can move to try-with-resources
    }
  }

  /** A generic functional interface for an operation that may throw an exception. */
  @FunctionalInterface
  public interface ExceptionThrowingCallable<V, E extends Exception> {
    V call() throws E;
  }

  /**
   * Times the given exception throwing operation and returns its result. Applies the outcome tag
   * indicating success or error of the operation. If the operation throws an exception, the
   * exception tag is also applied and the exception is re-thrown.
   *
   * @param sample the Timer.ResourceSample to use for timing
   * @param operation the operation to time
   * @param <V> the type of the operation's result
   * @param <E> the type of the exception the operation may throw
   * @return the result of the operation
   * @throws E if the operation throws an exception
   */
  public static <V, E extends Exception> V timedExceptionThrowingOperation(
      Timer.ResourceSample sample, ExceptionThrowingCallable<V, E> operation) throws E {
    try {
      try {
        final V ret = operation.call();
        sample.tags(Tags.of(TAG_OUTCOME_SUCCESS, TAG_EXCEPTION_NONE));
        return ret;
      } catch (final Exception ex) {
        sample.tags(
            Tags.of(TAG_OUTCOME_ERROR, Tag.of(TAG_EXCEPTION_KEY, ex.getClass().getSimpleName())));
        throw ex;
      }
    } finally {
      sample.close(); // after moving to JAVA 11 this can move to try-with-resources
    }
  }

  /**
   * Times the given operation. Applies the outcome tag indicating success or error of the
   * operation. If the operation throws an exception, the exception tag is also applied and the
   * exception is re-thrown.
   *
   * @param sample the Timer.ResourceSample to use for timing
   * @param operation the operation to time
   */
  public static void timedOperation(Timer.ResourceSample sample, Runnable operation) {
    try {
      try {
        operation.run();
        sample.tags(Tags.of(TAG_OUTCOME_SUCCESS, TAG_EXCEPTION_NONE));
      } catch (final Exception ex) {
        sample.tags(
            Tags.of(TAG_OUTCOME_ERROR, Tag.of(TAG_EXCEPTION_KEY, ex.getClass().getSimpleName())));
        throw ex;
      }
    } finally {
      sample.close(); // after moving to JAVA 11 this can move to try-with-resources
    }
  }

  private TimerUtils() {
    // private constructor to prevent instantiation
  }
}
