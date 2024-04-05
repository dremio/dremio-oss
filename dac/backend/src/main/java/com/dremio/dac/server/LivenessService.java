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
package com.dremio.dac.server;

import static com.dremio.telemetry.api.metrics.Metrics.MetricServletFactory.createMetricsServlet;

import com.dremio.common.liveness.LiveHealthMonitor;
import com.dremio.config.DremioConfig;
import com.dremio.service.Service;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Responds to HTTP requests on a special 'liveness' port bound to the loopback interface In the
 * future, can be expanded to check on the health of the internal workings of the system
 */
public class LivenessService implements Service {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(LivenessService.class);

  private static final int ACCEPT_QUEUE_BACKLOG = 1;
  private static final int NUM_ACCEPTORS = 1;
  private static final int NUM_SELECTORS = 1;
  private static final int NUM_REQUEST_THREADS = 1;
  private static final int NUM_USER_THREADS = 1;
  private static final int MAX_THREADS =
      NUM_ACCEPTORS + NUM_SELECTORS + NUM_REQUEST_THREADS + NUM_USER_THREADS;

  private final DremioConfig config;
  private final boolean livenessEnabled;

  private final Server embeddedLivenessJetty = new Server(new QueuedThreadPool(MAX_THREADS));
  private int livenessPort;
  private long pollCount;
  private final List<LiveHealthMonitor> healthMonitors = new ArrayList<>();

  public LivenessService(DremioConfig config) {
    this.config = config;
    this.livenessEnabled = config.getBoolean(DremioConfig.LIVENESS_ENABLED);
    pollCount = 0;
  }

  /**
   * adds a health monitor check to the list of checks
   *
   * @param healthMonitor
   */
  public void addHealthMonitor(LiveHealthMonitor healthMonitor) {
    Preconditions.checkArgument(healthMonitor != null, "Health monitor cannot be null");
    this.healthMonitors.add(healthMonitor);
  }

  @Override
  public void start() throws Exception {
    if (!livenessEnabled) {
      logger.info("Liveness service disabled");
      return;
    }
    final ServerConnector serverConnector =
        new ServerConnector(embeddedLivenessJetty, NUM_ACCEPTORS, NUM_SELECTORS);
    serverConnector.setPort(config.getInt(DremioConfig.LIVENESS_PORT));
    serverConnector.setHost(config.getString(DremioConfig.LIVENESS_HOST));
    serverConnector.setAcceptQueueSize(ACCEPT_QUEUE_BACKLOG);
    embeddedLivenessJetty.addConnector(serverConnector);

    ServletHandler handler = new ServletHandler();
    embeddedLivenessJetty.setHandler(handler);

    handler.addServletWithMapping(new ServletHolder(new LivenessServlet()), "/live");
    handler.addServletWithMapping(new ServletHolder(createMetricsServlet()), "/metrics");

    embeddedLivenessJetty.start();
    livenessPort = serverConnector.getLocalPort();
    logger.info("Started liveness service on port {}", livenessPort);
  }

  @Override
  public void close() throws Exception {
    if (!livenessEnabled) {
      return;
    }
    embeddedLivenessJetty.stop();
  }

  public boolean isLivenessServiceEnabled() {
    return livenessEnabled;
  }

  public long getPollCount() {
    return pollCount;
  }

  public int getLivenessPort() {
    return livenessPort;
  }

  /** Servlet used to respond to liveness requests. Simply returns a 'yes, I'm alive' */
  public class LivenessServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      pollCount++;
      for (LiveHealthMonitor hm : healthMonitors) {
        if (!hm.isHealthy()) {
          // return error code 500
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          return;
        }
      }
      response.setStatus(HttpServletResponse.SC_OK);
    }
  }
}
