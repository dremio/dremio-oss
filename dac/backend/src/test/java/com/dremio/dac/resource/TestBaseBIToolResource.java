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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.dremio.dac.server.WebServer;
import com.dremio.options.TypeValidators;

/**
 * Tests for class {@link BaseBIToolResource}
 */
@RunWith(Parameterized.class)
public class TestBaseBIToolResource {
  @Parameterized.Parameters(name="{0}")
  public static Object[] getTestCases() {
    return new Object[]{
      new String[] {"nullHost", null, null},
      new String[] {"localHost", "localhost", "localhost"},
      new String[] {"localHostWithPort", "localhost:9047", "localhost"},
      new String[] {"ipAsServer", "127.0.0.1", "127.0.0.1"},
      new String[] {"ipWithPortAsServer", "127.0.0.1:9047", "127.0.0.1"}
    };
  }

  private final String inputHost;
  private final String expectedHost;
  private BaseBIToolResource resource;

  public TestBaseBIToolResource(String testName, String inputHost, String expectedHost) {
    this.inputHost = inputHost;
    this.expectedHost = expectedHost;
  }

  @Before
  public void setUp() {
    resource = new BaseBIToolResource(null, null, "a/b", null, null) {
      @Override
      protected TypeValidators.BooleanValidator getClientToolOption() {
        return null;
      }
    };
  }

  @Test
  public void verifyHeaders() {
    final Response response = resource.buildResponseWithHost(Response.ok(), inputHost).build();
    if (expectedHost == null) {
      assertFalse(response.getHeaders().containsKey(WebServer.X_DREMIO_HOSTNAME));
      return;
    }
    assertEquals(expectedHost, response.getHeaders().get(WebServer.X_DREMIO_HOSTNAME).get(0));
  }
}
