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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestMetricsInstrumenter {
  private static final String SERVICE_NAME = "serviceName";
  private static final String OPERATION_NAME = "operationName";
  private static final String TIME_METRIC_NAME =
    Metrics.join(SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.TIME_METRIC_SUFFIX);
  private static final String COUNT_METRIC_NAME =
    Metrics.join(SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.COUNT_METRIC_SUFFIX);
  private static final String ERROR_METRIC_NAME =
    Metrics.join(SERVICE_NAME, OPERATION_NAME, MetricsInstrumenter.ERROR_METRIC_SUFFIX);
  private static final String EXPECTED_CALLABLE_RESULT = "cookie";

  @Mock
  private Counter counter;

  @Mock
  private Counter errorCounter;

  @Mock
  private Timer timer;

  @Mock
  private Timer.TimerContext timerContext;

  @Mock
  private MetricsProvider mockProvider;

  private MetricsInstrumenter metrics;

  @Before
  public void setup() {
    when(timer.start()).thenReturn(timerContext);
    when(mockProvider.timer(TIME_METRIC_NAME)).thenReturn(timer);
    when(mockProvider.counter(COUNT_METRIC_NAME)).thenReturn(counter);
    when(mockProvider.counter(ERROR_METRIC_NAME)).thenReturn(errorCounter);

    metrics = new MetricsInstrumenter(SERVICE_NAME, mockProvider);
  }

  @Test
  public void givenASuccessfulRunMetricsAreLogged() {
    DummyRunnable succesfulOperation = DummyRunnable.success();

    metrics.log(OPERATION_NAME, succesfulOperation);

    assertSuccessMetricsLogged();
    succesfulOperation.assertSuccessfulRun();
  }

  @Test
  public void canInstrumentASuccessfulRunnable() {
    DummyRunnable succesfulOperation = DummyRunnable.success();

    Runnable instrumented = metrics.instrument(OPERATION_NAME, succesfulOperation);

    verifyNoInteractions(counter, errorCounter, timerContext);

    instrumented.run();

    assertSuccessMetricsLogged();
    succesfulOperation.assertSuccessfulRun();
  }

  @Test
  public void canInstrumentASuccessfulCallable() throws Exception {
    Callable<String> callable = () -> EXPECTED_CALLABLE_RESULT;

    Callable<String> instrumented = metrics.instrument(OPERATION_NAME, callable);

    verifyNoInteractions(counter, errorCounter, timerContext);

    String result = instrumented.call();

    assertThat(result).isEqualTo(EXPECTED_CALLABLE_RESULT);
    assertSuccessMetricsLogged();
  }

  @Test
  public void givenASuccessfulCallMetricsAreLogged() throws Exception {
    String callableResult = metrics.log(OPERATION_NAME, () -> EXPECTED_CALLABLE_RESULT);

    assertSuccessMetricsLogged();
    assertThat(callableResult).isEqualTo(EXPECTED_CALLABLE_RESULT);
  }

  @Test
  public void givenAFailingRunMetricsAreLogged() {
    Runnable failingOperation = () -> { throw new DummyRuntimeException(); };

    assertThatThrownBy(() -> metrics.log(OPERATION_NAME, failingOperation))
      .isInstanceOf(DummyRuntimeException.class);

    assertErrorMetricsLogged();
  }

  @Test
  public void givenAFailingCallMetricsAreLogged() {
    Callable<String> failingOperation = () -> {
      throw new DummyRuntimeException();
    };

    assertThatThrownBy(() -> metrics.log(OPERATION_NAME, failingOperation))
      .isInstanceOf(DummyRuntimeException.class);

    assertErrorMetricsLogged();
  }

  @Test
  public void canLogFailingOperationWithCheckedException() {
    Callable<String> failingOperation = () -> { throw new DummyCheckedException(); };

    assertThatThrownBy(() -> metrics.log(OPERATION_NAME, failingOperation))
      .hasCause(new DummyCheckedException());

    assertErrorMetricsLogged();
  }

  private void assertSuccessMetricsLogged() {
    verify(counter, times(1)).increment();
    verify(timerContext, times(1)).close();
    verifyNoInteractions(errorCounter);
  }

  private void assertErrorMetricsLogged() {
    verify(counter, times(1)).increment();
    verify(timerContext, times(1)).close();
    verify(errorCounter, times(1)).increment();
  }

  private static final class DummyRunnable implements Runnable {
    private boolean successfulRun = false;

    @Override
    public void run() {
      successfulRun = true;
    }

    static DummyRunnable success() {
      return new DummyRunnable();
    }

    void assertSuccessfulRun() {
      assertThat(successfulRun).as("Runnable wasn't run").isTrue();
    }
  }

  private static final class DummyRuntimeException extends RuntimeException {

  }

  private static final class DummyCheckedException extends Exception {

  }
}
