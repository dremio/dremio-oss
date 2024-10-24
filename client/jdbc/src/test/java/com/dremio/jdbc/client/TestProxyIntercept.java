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
package com.dremio.jdbc.client;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.exec.rpc.proxy.ProxyConfig;
import com.dremio.jdbc.test.JdbcAssert;
import io.netty.handler.proxy.DremioSocks5ProxyServer;
import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProxyIntercept extends BaseTestQuery {
  private static final String SOCKS_PROXY_USERNAME = "testUser";
  private static final String SOCKS_PROXY_PASSWORD = "testPassword";

  @BeforeClass
  public static void setup() throws Exception {
    // Skip since ProxyServer does not work with the bouncy castle fips library
    // The published driver is not from the MapR build anyway
    assumeNonMaprProfile();
  }

  private static Properties newProxyProperties(DremioSocks5ProxyServer proxy) {
    Properties properties = JdbcAssert.getDefaultProperties();
    properties.put(ProxyConfig.SOCKS_PROXY_HOST, proxy.address().getHostName());
    properties.put(ProxyConfig.SOCKS_PROXY_PORT, String.valueOf(proxy.address().getPort()));
    return properties;
  }

  @Test
  public void testProxyNoAuth() throws Exception {
    final InetSocketAddress destinationAddress =
        new InetSocketAddress("localhost", getFirstCoordinatorEndpoint().getUserPort());

    try (DremioSocks5ProxyServer proxyNoAuth = new DremioSocks5ProxyServer(destinationAddress)) {
      Properties properties = newProxyProperties(proxyNoAuth);

      DriverManager.getConnection(getJDBCURL(), properties);
      proxyNoAuth.checkExceptions();
      Assert.assertTrue(proxyNoAuth.interceptedConnection());
    }
  }

  @Test
  public void testProxyAuth() throws Exception {
    final InetSocketAddress destinationAddress =
        new InetSocketAddress("localhost", getFirstCoordinatorEndpoint().getUserPort());

    try (DremioSocks5ProxyServer proxyWithAuth =
        new DremioSocks5ProxyServer(
            destinationAddress, SOCKS_PROXY_USERNAME, SOCKS_PROXY_PASSWORD)) {

      Properties properties = newProxyProperties(proxyWithAuth);
      properties.put(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USERNAME);
      properties.put(ProxyConfig.SOCKS_PROXY_PASSWORD, SOCKS_PROXY_PASSWORD);

      DriverManager.getConnection(getJDBCURL(), properties);

      proxyWithAuth.checkExceptions();
      Assert.assertTrue(proxyWithAuth.interceptedConnection());
    }
  }

  @Test
  public void testProxyWrongPassword() {
    final InetSocketAddress destinationAddress =
        new InetSocketAddress("localhost", getFirstCoordinatorEndpoint().getUserPort());
    try (DremioSocks5ProxyServer proxyWithAuth =
        new DremioSocks5ProxyServer(
            destinationAddress, SOCKS_PROXY_USERNAME, SOCKS_PROXY_PASSWORD)) {

      Properties properties = newProxyProperties(proxyWithAuth);
      properties.put(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USERNAME);
      properties.put(ProxyConfig.SOCKS_PROXY_PASSWORD, "gnarly");

      assertThatThrownBy(() -> DriverManager.getConnection(getJDBCURL(), properties))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("ProxyConnectException");

      proxyWithAuth.checkExceptions();
    }
  }
}
