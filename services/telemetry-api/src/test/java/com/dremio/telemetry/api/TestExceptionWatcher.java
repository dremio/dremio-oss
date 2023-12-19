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
package com.dremio.telemetry.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.dremio.telemetry.api.Telemetry.ExceptionWatcher;

/**
 * Tests Telemetry.ExceptionWatcher to ensure exceptions will only be reported once.
 */
public class TestExceptionWatcher {

  private static final class TestConsumer implements Consumer<Exception> {

    private List<Exception> calledWith = new ArrayList<>();

    @Override
    public void accept(Exception e) {
      calledWith.add(e);
    }
  }

  private ExceptionWatcher exceptionWatcher;
  private TestConsumer testConsumer;

  private Exception e1() {
    return new Exception("Test exception 1");
  }

  private Exception e2() {
    return new Exception("Test exception 2");
  }

  @Before
  public void setup() {
    testConsumer = new TestConsumer();
    exceptionWatcher = new ExceptionWatcher(testConsumer);
  }

  private void notify(Exception e) {
    exceptionWatcher.notify(e);
  }

  private void reset() {
    exceptionWatcher.reset();
  }

  private void assertInvokeOrder(Exception... expecteds) {
    assertEquals(expecteds.length, testConsumer.calledWith.size());
    for (int i = 0; i < expecteds.length; ++i) {
      assertEquals(expecteds[i].getMessage(), testConsumer.calledWith.get(i).getMessage());
    }
  }


  @Test
  public void invokesCallableOnlyOnFirstOccurrenceOfFailureType() {
    notify(e1());
    notify(e1());

    assertInvokeOrder(e1());
  }

  @Test
  public void invokesCallableWhenExceptionChanges() {
    notify(e1());
    notify(e2());
    notify(e1());

    assertInvokeOrder(e1(), e2(), e1());
  }

  @Test
  public void invokesCallableOnAnyExceptionAfterReset() {
    notify(e1());
    reset();
    notify(e2());

    assertInvokeOrder(e1(), e2());
  }

  @Test
  public void invokesCallableOnSameExceptionAfterReset() {
    notify(e1());
    reset();
    notify(e1());

    assertInvokeOrder(e1(), e1());
  }

  @Test
  public void invokesCallableAfterInitialSuccess() {
    reset();
    reset();
    reset();
    notify(e2());

    assertInvokeOrder(e2());
  }
}
