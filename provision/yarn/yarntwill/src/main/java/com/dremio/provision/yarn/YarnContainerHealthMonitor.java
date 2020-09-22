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
package com.dremio.provision.yarn;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpStatus;

import com.dremio.common.liveness.LiveHealthMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

/**
 * When YARN provisioning is used, this health monitor checks
 * whether YARN container hosting Dremio executor is healthy or not
 */
public class YarnContainerHealthMonitor implements LiveHealthMonitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(YarnContainerHealthMonitor.class);
  private boolean isHealthy;
  private YarnContainerHealthMonitorThread yarnContainerHealthMonitorThread;
  private Thread healthMonitorThread;

  public YarnContainerHealthMonitor() {
    isHealthy = true;
    yarnContainerHealthMonitorThread = new YarnContainerHealthMonitorThread();
    healthMonitorThread = new Thread(yarnContainerHealthMonitorThread);
    healthMonitorThread.start();
  }

  @Override
  public boolean isHealthy() {
    //TODO DX-20576: We are currently always returning true
    // In the future, this method should return the health of the container
    // as implemented in YarnContainerHealthMonitorThread#isContainerHealthy()
    return isHealthy;
  }

  /**
   * Thread for monitoring the health of the container inside which the Dremio executor is running
   * <p>
   * This is established by making an HTTP GET call to one of the NodeManager's REST APIs which returns
   * the state of the container
   * If the state is either one of RUNNING or NEW, it is considered to be healthy
   */
  static class YarnContainerHealthMonitorThread implements Runnable {
    private static final String YARN_CONTAINER_PARAMETER = "-Dyarn.container";
    private final String nodeManagerWebappAddress;
    private final String containerID;
    private final String nodeManagerURL;
    private boolean isHealthy;
    private String previousContainerState;
    private Throwable exception;

    public YarnContainerHealthMonitorThread() {
      Configuration configuration = new YarnConfiguration();
      nodeManagerWebappAddress = configuration.get(YarnConfiguration.NM_WEBAPP_ADDRESS);
      containerID = System.getProperty(YARN_CONTAINER_PARAMETER);
      nodeManagerURL = "http://" + nodeManagerWebappAddress + "/ws/v1/node/containers/" + containerID;
    }

    public YarnContainerHealthMonitorThread(String nodeManagerWebappAddress, String containerID) {
      this.nodeManagerWebappAddress = nodeManagerWebappAddress;
      this.containerID = containerID;
      nodeManagerURL = "http://" + nodeManagerWebappAddress + "/ws/v1/node/containers/" + containerID;
    }

    public boolean isHealthy() {
      return isHealthy;
    }

    @Override
    public void run() {
      while (isHealthy) {
        try {
          isContainerHealthy();
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    /**
     * Method to make the HTTP call and log the state of the container
     */
    private void isContainerHealthy() {
      try {
        URL url = new URL(nodeManagerURL);
        Stopwatch stopwatch = Stopwatch.createStarted();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        String currentContainerState = getContainerState(connection);
        if (currentContainerState == null) {
          logger.error("Container with id: {} does not exist!", containerID);
          return;
        }
        stopwatch.stop();
        logger.debug("Retrieving container state from NodeManager URL: {} took {} microseconds with response code {}",
          nodeManagerURL, stopwatch.elapsed(TimeUnit.MICROSECONDS), connection.getResponseCode());
        if (!currentContainerState.equals(previousContainerState)) {
          logger.info("Container state changed. Previous: {}, Current: {}", previousContainerState, currentContainerState);
          previousContainerState = currentContainerState;
        }
      } catch (IOException e) {
        if (!e.getCause().equals(exception)) {
          logger.error("Error occurred while connecting to NodeManager!", e);
          exception = e.getCause();
        }
      }
    }

    /**
     * Method to fetch the state of the container
     *
     * @param connection {@link HttpURLConnection} used to make the NodeManager REST API call
     * @return container state which can be either of the states in {@link ContainerState}
     */
    @VisibleForTesting
    String getContainerState(HttpURLConnection connection) throws IOException {
      if (connection.getResponseCode() == HttpStatus.SC_NOT_FOUND) {
        isHealthy = false;
        return null;
      }
      ObjectMapper mapper = new ObjectMapper();
      JsonNode containerInfoNode = mapper.readTree(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
      String containerState = containerInfoNode.get("container").get("state").asText();
      isHealthy = ContainerState.NEW.name().equals(containerState)
        || ContainerState.RUNNING.name().equals(containerState);
      return containerState;
    }
  }
}
