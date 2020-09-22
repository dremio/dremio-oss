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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;

import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link YarnContainerHealthMonitor}
 */
public class TestYarnContainerHealthMonitor {
  private YarnContainerHealthMonitor.YarnContainerHealthMonitorThread healthMonitorThread;
  private HttpURLConnection connection;

  @Before
  public void setup() {
    healthMonitorThread = new YarnContainerHealthMonitor.YarnContainerHealthMonitorThread(
      "0.0.0.0:8042", "container_1234");
    connection = mock(HttpURLConnection.class);
  }

  @Test
  public void testUnavailableContainer() throws Exception {
    when(connection.getResponseCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
    assertNull(healthMonitorThread.getContainerState(connection));
    assertFalse(healthMonitorThread.isHealthy());
  }

  @Test
  public void testHealthyContainer() throws Exception {
    String containerInfoJson = "{\"container\":{\"user\":\"dremio\",\"state\":\"NEW\"}}";
    InputStream targetStream = new ByteArrayInputStream(containerInfoJson.getBytes());
    when(connection.getInputStream()).thenReturn(targetStream);
    assertEquals(ContainerState.NEW.name(), healthMonitorThread.getContainerState(connection));
    assertTrue(healthMonitorThread.isHealthy());
  }

  @Test
  public void testUnhealthyContainer() throws Exception {
    String containerInfoJson = "{\"container\":{\"state\":\"COMPLETE\",\"user\":\"root\"}}";
    InputStream targetStream = new ByteArrayInputStream(containerInfoJson.getBytes());
    when(connection.getInputStream()).thenReturn(targetStream);
    assertEquals(ContainerState.COMPLETE.name(), healthMonitorThread.getContainerState(connection));
    assertFalse(healthMonitorThread.isHealthy());
  }
}
