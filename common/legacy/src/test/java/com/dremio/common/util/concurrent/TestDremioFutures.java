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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Test Dremio Futures */
public class TestDremioFutures {

  /** Test basic value retrieval. */
  @Test
  public void testBasic() {
    SettableFuture<Integer> future = SettableFuture.create();
    Integer expectedValue = 1234;
    future.set(expectedValue);

    try {
      Integer result =
          DremioFutures.getChecked(future, TestException.class, TestDremioFutures::testMapper);
      assertEquals(result, expectedValue);
    } catch (TestException ignored) {
      throw new AssertionError();
    }
  }

  /** Test timeout Exception is properly thrown by forcing timeout with a Callback. */
  @Test
  public void testTimeout() {
    SettableFuture future = SettableFuture.create();
    Futures.addCallback(
        future,
        new FutureCallback<Integer>() {
          @Override
          public void onSuccess(@Nullable Integer result) {
            try {
              Thread.sleep(10000);
            } catch (InterruptedException ignored) {
            }
            future.set(result);
          }

          @Override
          public void onFailure(Throwable t) {}
        },
        MoreExecutors.directExecutor());

    try {
      DremioFutures.getChecked(
          future, TestException.class, 1, TimeUnit.MILLISECONDS, TestDremioFutures::testMapper);
      throw new AssertionError();
    } catch (TimeoutException e) {
      // Success
    } catch (Exception e) {
      throw new AssertionError();
    }
  }

  /** Tests exception mapping of ExecutionException to TestException. */
  @Test
  public void testMappingCause() {
    Future future = mock(Future.class);
    Throwable cause = new TestException("inner exception");
    try {
      when(future.get()).thenThrow(new ExecutionException("outer exception", cause));
    } catch (InterruptedException | ExecutionException ignored) {
      throw new AssertionError();
    }
    try {
      DremioFutures.getChecked(future, TestException.class, TestDremioFutures::testMapper);
      throw new AssertionError();
    } catch (TestException e) {
      assertSame(e.getCause(), cause);
    } catch (Exception e) {
      throw new AssertionError();
    }
  }

  private static TestException testMapper(Throwable t) {
    return new TestException("Test Exception", t);
  }

  private static class TestException extends Exception {

    @SuppressWarnings("unused")
    public TestException(String message, Throwable cause) {
      super(message, cause);
    }

    public TestException(String message) {
      super(message);
    }
  }
}
