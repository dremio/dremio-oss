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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.dremio.common.ProcessExit;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.beans.DremioExitCodes;
import com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.Duration;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To be used only on executor nodes when an embedded ZooKeeper is also in use. Detects an absent
 * master conduit endpoint (a symptom of erroneous ZooKeeper client state) and exits the executor
 * process in the expectation that it will be automatically restarted - bringing along a clean
 * ZooKeeper client. See DX-87859 for more details.
 */
public interface EmbeddedZooKeeperWatchdog {

  Logger logger = LoggerFactory.getLogger(EmbeddedZooKeeperWatchdog.class);

  static Provider<CoordinationProtos.NodeEndpoint> watch(
      Provider<CoordinationProtos.NodeEndpoint> endpoint,
      int errorCountThreshold,
      String errorDurationThreshold) {
    return new ErrorDetectingEndpointProvider(
        endpoint, errorCountThreshold, Duration.parse(errorDurationThreshold));
  }

  class ErrorDetectingEndpointProvider implements Provider<CoordinationProtos.NodeEndpoint> {
    /** The time that an error was first detected. Will be reset if the error condition clears. */
    private volatile long firstDetectionTs;

    /**
     * The count of sequential errors that we've detected. Will be reset if the error condition
     * clears.
     */
    private volatile int detections;

    /**
     * True if we've ever successfully connected in the lifetime of this process. To filter out
     * races between coordinator and executors on a cluster start-up.
     */
    private volatile boolean hasPreviouslyConnected;

    /** The actual source of the master endpoint. */
    private final Provider<CoordinationProtos.NodeEndpoint> endpointProvider;

    /** Maximum number of sequential errors that we'll tolerate. */
    private final int errorCountThreshold;

    /**
     * Minimum duration that we'll tolerate sequential errors. We won;t exit before the duration has
     * expired.
     */
    private final Duration errorDurationThreshold;

    private final ApplicationTerminal terminal;
    private final Clock clock;

    ErrorDetectingEndpointProvider(
        Provider<CoordinationProtos.NodeEndpoint> endpointProvider,
        int errorCountThreshold,
        Duration errorDurationThreshold,
        ApplicationTerminal terminal,
        Clock clock) {
      Preconditions.checkNotNull(endpointProvider, "endpointProvider must be non-null");
      Preconditions.checkArgument(
          errorCountThreshold > 0, "errorCountThreshold must be greater than 0");
      Preconditions.checkArgument(
          errorDurationThreshold.getSeconds() > 0,
          "errorDurationThreshold must be at least 1 second");
      Preconditions.checkNotNull(terminal, "terminal must be non-null");
      Preconditions.checkNotNull(clock, "clock must be non-null");
      this.endpointProvider = endpointProvider;
      this.errorCountThreshold = errorCountThreshold;
      this.errorDurationThreshold = errorDurationThreshold;
      this.terminal = terminal;
      this.clock = clock;
      logger.warn(
          "Additional ZooKeeper connection error mitigation applied to endpoint provider. maxErrors: {}, errorDuration {}",
          errorCountThreshold,
          errorDurationThreshold);
    }

    ErrorDetectingEndpointProvider(
        Provider<CoordinationProtos.NodeEndpoint> endpointProvider,
        int errorCountThreshold,
        Duration errorDurationThreshold) {
      this(
          endpointProvider,
          errorCountThreshold,
          errorDurationThreshold,
          ApplicationTerminal.PROCESS_EXIT,
          Clock.systemUTC());
    }

    @Override
    public synchronized CoordinationProtos.NodeEndpoint get() {
      CoordinationProtos.NodeEndpoint endpoint = endpointProvider.get();
      if (hasPreviouslyConnected && endpoint == null) {
        if (detections == 0) {
          firstDetectionTs = clock.millis();
        }
        detections++;
        long now = clock.millis();
        long errorDurationMs = now - firstDetectionTs;
        logger.info(
            "Endpoint provider error detected. Error count: {}/{}, Error duration: {}s/{}s",
            detections,
            errorCountThreshold,
            MILLISECONDS.toSeconds(errorDurationMs),
            errorDurationThreshold.toSeconds());
        if (detections >= errorCountThreshold
            && errorDurationMs > errorDurationThreshold.toMillis()) {
          logger.error(
              "Endpoint provider error thresholds exhausted. Exiting process in the expectation that it will be restarted.");
          terminal.exit();
        }
      } else {
        hasPreviouslyConnected = true;
        if (detections > 0) {
          reset();
        }
        return endpoint;
      }
      return null;
    }

    private void reset() {
      firstDetectionTs = 0L;
      detections = 0;
      logger.info("Endpoint provider operational - resetting error conditions.");
    }
  }

  interface ApplicationTerminal {
    ApplicationTerminal PROCESS_EXIT =
        () ->
            ProcessExit.exit(
                "Embedded ZooKeeper session was lost.", DremioExitCodes.LOST_ZOOKEEPER_SESSION);

    void exit();
  }
}
