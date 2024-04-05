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

package com.dremio.common.util.concurrent;

import com.google.common.util.concurrent.Futures;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Dremio version of Guava's Futures.getChecked that behaves more similarly to CheckedFutures from
 * Guava 20.0. Throws TimeoutException directly. Allows for custom exception mapping.
 */
public final class DremioFutures {

  private DremioFutures() {}

  /**
   * Dremio version of Guava's Futures.getChecked without timeout.
   *
   * @param future Future to get from
   * @param exceptionClass Exception Class to throw
   * @param mapper Function to map original exception to exceptionClass
   */
  public static <T, X extends Exception> T getChecked(
      Future<T> future, Class<X> exceptionClass, Function<? super Throwable, ? extends X> mapper)
      throws X {
    try {
      return Futures.getChecked(future, ExecutionException.class);
    } catch (ExecutionException e) {
      try {
        handleException(e, exceptionClass, mapper);
        throw new AssertionError();
      } catch (TimeoutException ex) {
        throw new AssertionError();
      }
    }
  }

  /**
   * Dremio version of Guava's Futures.getChecked with timeout. Throws TimeoutException directly
   * instead of hiding it.
   *
   * @param future Future to get from
   * @param exceptionClass Exception Class to throw
   * @param timeout Timeout amount
   * @param unit Timeout units
   * @param mapper Function to map original exception to exceptionClass
   */
  public static <T, X extends Exception> T getChecked(
      Future<T> future,
      Class<X> exceptionClass,
      long timeout,
      TimeUnit unit,
      Function<? super Throwable, ? extends X> mapper)
      throws TimeoutException, X {
    try {
      return Futures.getChecked(future, ExecutionException.class, timeout, unit);
    } catch (ExecutionException e) {
      handleException(e, exceptionClass, mapper);
      throw new AssertionError();
    }
  }

  /** Apply mapper to ExecutionException or its cause. */
  private static <X extends Exception> void handleException(
      ExecutionException e,
      Class<X> exceptionClass,
      Function<? super Throwable, ? extends X> mapper)
      throws TimeoutException, X {
    Throwable cause = e.getCause();
    if (exceptionClass.isInstance(cause)
        || cause instanceof RuntimeException
        || cause instanceof Error) {
      throw mapper.apply(cause);
    } else if (cause instanceof TimeoutException) {
      throw (TimeoutException) cause;
    } else {
      throw mapper.apply(e);
    }
  }
}
