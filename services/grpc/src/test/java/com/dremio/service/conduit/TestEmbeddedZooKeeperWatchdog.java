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
package com.dremio.service.conduit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.conduit.EmbeddedZooKeeperWatchdog.ApplicationTerminal;
import com.dremio.service.conduit.EmbeddedZooKeeperWatchdog.ErrorDetectingEndpointProvider;
import com.google.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

class TestEmbeddedZooKeeperWatchdog {

  @Nested
  @DisplayName("Error detection")
  @ExtendWith(MockitoExtension.class)
  class TestErrorDetectingEndpointProvider {
    @Mock private ApplicationTerminal terminal;
    @Mock private Clock clock;
    @Mock private Provider<CoordinationProtos.NodeEndpoint> endpointProvider;
    private final CoordinationProtos.NodeEndpoint endpoint =
        CoordinationProtos.NodeEndpoint.getDefaultInstance();
    private ErrorDetectingEndpointProvider underTest;

    @BeforeEach
    void injectMocks() {
      underTest =
          new ErrorDetectingEndpointProvider(
              endpointProvider, 3, Duration.ofSeconds(10), terminal, clock);
    }

    @Test
    void exitsWhenErrorThresholdsExhausted() {
      when(clock.millis()).thenReturn(0L, 0L, 1L, 10001L);
      when(endpointProvider.get()).thenReturn(endpoint, null, null, null);

      assertThat(underTest.get()).isEqualTo(endpoint);

      // clock 0 + 0, err count: 1
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);

      // clock 1, err count: 2
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);

      // clock 10001, err count: 3
      assertThat(underTest.get()).isNull();
      verify(terminal).exit();
    }

    @Test
    void exitsWhenErrorCountThresholdExhausted() {
      // Error duration threshold exceeded on first error
      when(clock.millis()).thenReturn(0L, 999999L);
      when(endpointProvider.get()).thenReturn(endpoint, null, null, null);

      assertThat(underTest.get()).isEqualTo(endpoint);
      IntStream.range(0, 2)
          .forEach(
              i -> {
                assertThat(underTest.get()).isNull();
                verifyNoInteractions(terminal);
              });

      assertThat(underTest.get()).isNull();
      verify(terminal).exit();
    }

    @Test
    void doesNotExitWhenNeverConnectedAndErrorThresholdsExhaustedButNoInitialConnection() {
      when(clock.millis()).thenReturn(0L, 999999L);
      when(endpointProvider.get()).thenReturn(null, null, null);

      // clock 0 + 0, err count: 1
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);

      // clock 1, err count: 2
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);

      // clock 10001, err count: 3
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);
    }

    @Test
    void doesNotExitWhenErrorCountThresholdExhaustedButNoInitialConnection() {
      // Error duration threshold exceeded on first error
      when(clock.millis()).thenReturn(0L, 999999L);
      when(endpointProvider.get()).thenReturn(null, null, null);

      IntStream.range(0, 2)
          .forEach(
              i -> {
                assertThat(underTest.get()).isNull();
                verifyNoInteractions(terminal);
              });

      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);
    }

    @Test
    void recovers() {
      // Error duration threshold exceeded on first error
      when(clock.millis()).thenReturn(0L, 999999L);
      when(endpointProvider.get()).thenReturn(endpoint, null, null, endpoint, null, null);

      assertThat(underTest.get()).isEqualTo(endpoint);
      assertThat(underTest.get()).isNull();
      assertThat(underTest.get()).isNull();
      assertThat(underTest.get()).isEqualTo(endpoint); // resets
      assertThat(underTest.get()).isNull();
      assertThat(underTest.get()).isNull();
      verifyNoInteractions(terminal);
    }
  }
}
