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
package com.dremio.dac.explore.bi;

import static com.dremio.dac.explore.bi.BIToolsConstants.EXPORT_HOSTNAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.dremio.dac.server.WebServer;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Unit tests for {@link BaseBIToolMessageBodyGenerator}.
 */
public class TestBaseBIToolMessageBodyGenerator {
  private static final String TEST_DEFAULT_HOSTNAME = "test.default.hostname";
  private static final String TEST_BI_TOOL_HOSTNAME_OVERRIDE = "test.bi.tools.hostname";
  private static final String TEST_DREMIO_WEB_SERVER_HOSTNAME_OVERRIDE = "test.dremio.web.server.hostname";

  private static final OptionManager OPTION_MANAGER = mock(OptionManager.class);
  private static final BaseBIToolMessageBodyGenerator GENERATOR = new MockBaseBIToolMessageBodyGenerator(
    CoordinationProtos.NodeEndpoint.newBuilder().setAddress(TEST_DEFAULT_HOSTNAME).build(),
    OPTION_MANAGER);
  private static final BaseBIToolMessageBodyGenerator SPY = spy(GENERATOR);

  private static class MockBaseBIToolMessageBodyGenerator extends BaseBIToolMessageBodyGenerator {
    protected MockBaseBIToolMessageBodyGenerator(CoordinationProtos.NodeEndpoint endpoint,
                                                 OptionManager optionManager) {
      super(endpoint, optionManager);
    }

    @Override
    public void writeTo(DatasetConfig datasetConfig, Class<?> aClass, Type type,
                        Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream)
      throws IOException, WebApplicationException {
      // Do nothing
    }
  }

  @Test
  public void testGetHostNameDefault() {
    // Setup
    when(OPTION_MANAGER.getOption(EXPORT_HOSTNAME)).thenReturn(null);

    // Test
    final String actual = SPY.getHostname(new MultivaluedHashMap<>());

    // Verify
    assertEquals(TEST_DEFAULT_HOSTNAME, actual);
  }

  @Test
  public void testGetHostNameWithDremioHostNameOverride() {
    // Setup
    when(OPTION_MANAGER.getOption(EXPORT_HOSTNAME)).thenReturn(null);
    final MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    httpHeaders.add(WebServer.X_DREMIO_HOSTNAME, TEST_DREMIO_WEB_SERVER_HOSTNAME_OVERRIDE);

    // Test
    final String actual = SPY.getHostname(httpHeaders);

    // Verify
    assertEquals(TEST_DREMIO_WEB_SERVER_HOSTNAME_OVERRIDE, actual);
  }

  @Test
  public void testGetHostNameWithBIToolHostNameOverride() {
    // Setup
    when(OPTION_MANAGER.getOption(BIToolsConstants.EXPORT_HOSTNAME))
      .thenReturn(TEST_BI_TOOL_HOSTNAME_OVERRIDE);

    // Test
    final String actual = SPY.getHostname(null);

    // Verify
    assertEquals(TEST_BI_TOOL_HOSTNAME_OVERRIDE, actual);
  }

  @Test
  public void testGetHostNameWithMultipleOverride() {
    // BI Tool export hostname override should take precedence over
    // Dremio web server hostname override

    // Setup
    when(OPTION_MANAGER.getOption(BIToolsConstants.EXPORT_HOSTNAME))
      .thenReturn(TEST_BI_TOOL_HOSTNAME_OVERRIDE);
    final MultivaluedMap<String, Object> httpHeaders = new MultivaluedHashMap<>();
    httpHeaders.add(WebServer.X_DREMIO_HOSTNAME, TEST_DREMIO_WEB_SERVER_HOSTNAME_OVERRIDE);

    // Test
    final String actual = SPY.getHostname(httpHeaders);

    // Verify
    assertEquals(TEST_BI_TOOL_HOSTNAME_OVERRIDE, actual);
  }
}
