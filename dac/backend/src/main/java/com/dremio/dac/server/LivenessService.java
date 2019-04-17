/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.dremio.config.DremioConfig;
import com.dremio.sabot.exec.TaskPoolInitializer;
import com.dremio.service.Service;

/**
 * Responds to HTTP requests on a special 'liveness' port bound to the loopback interface
 * In the future, can be expanded to check on the health of the internal workings of the system
 */
public class LivenessService implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LivenessService.class);

  private static final String LOOPBACK_INTERFACE = "127.0.0.1";
  private static final int ACCEPT_QUEUE_BACKLOG = 1;
  private static final int NUM_ACCEPTORS = 1;
  private static final int NUM_SELECTORS = 1;
  private static final int NUM_REQUEST_THREADS = 1;

  private final DremioConfig config;
  private final boolean livenessEnabled;

  private final Server embeddedLivenessJetty = new Server(new QueuedThreadPool(NUM_ACCEPTORS + NUM_SELECTORS + NUM_REQUEST_THREADS));
  private int livenessPort;
  private long pollCount;
  private final TaskPoolInitializer taskPoolInitializer;

  public LivenessService(DremioConfig config, TaskPoolInitializer taskPoolInitializer) {
    this.config = config;
    this.livenessEnabled = config.getBoolean(DremioConfig.LIVENESS_ENABLED);
    this.taskPoolInitializer = taskPoolInitializer;
    pollCount = 0;
  }

  @Override
  public void start() throws Exception {
    if (!livenessEnabled) {
      logger.info("Liveness service disabled");
      return;
    }
    final ServerConnector serverConnector = new ServerConnector(embeddedLivenessJetty, NUM_ACCEPTORS, NUM_SELECTORS);
    serverConnector.setPort(config.getInt(DremioConfig.LIVENESS_PORT));
    serverConnector.setHost(LOOPBACK_INTERFACE);
    serverConnector.setAcceptQueueSize(ACCEPT_QUEUE_BACKLOG);
    embeddedLivenessJetty.addConnector(serverConnector);

    ServletHandler handler = new ServletHandler();
    embeddedLivenessJetty.setHandler(handler);
    handler.addServletWithMapping(new ServletHolder(new LivenessServlet()), "/live");

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

  /**
   * Servlet used to respond to liveness requests. Simply returns a 'yes, I'm alive'
   */
  public class LivenessServlet extends HttpServlet
  {
    @Override
    protected void doGet(HttpServletRequest request,
                         HttpServletResponse response ) throws ServletException, IOException
    {
      pollCount++;
      if ((taskPoolInitializer != null) && !taskPoolInitializer.isTaskPoolHealthy()) {
        // return error code 500
        logger.info("One of the slicing threads is dead, returning an error");
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        return;
      }

      response.setStatus(HttpServletResponse.SC_OK);
    }
  }
}
