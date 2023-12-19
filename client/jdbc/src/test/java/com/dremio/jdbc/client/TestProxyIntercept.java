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

import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.exec.rpc.proxy.ProxyConfig;
import com.dremio.jdbc.SabotNodeRule;
import com.dremio.jdbc.test.JdbcAssert;

import io.netty.handler.proxy.DremioSocks5ProxyServer;

public class TestProxyIntercept {

  @ClassRule
  public static final SabotNodeRule sabotNode = new SabotNodeRule();
  private static final String SOCKS_PROXY_USERNAME = "testUser";
  private static final String SOCKS_PROXY_PASSWORD = "testPassword";

  @Test
  public void testProxyNoAuth() throws Exception {
    final InetSocketAddress destinationAddress = new InetSocketAddress("localhost", sabotNode.getPort());

    try(DremioSocks5ProxyServer proxyNoAuth = new DremioSocks5ProxyServer(destinationAddress)) {

      final String socksProxyHost = proxyNoAuth.address().getHostName();
      final String socksProxyPort = String.valueOf(proxyNoAuth.address().getPort());

      Properties properties = JdbcAssert.getDefaultProperties();
      properties.put(ProxyConfig.SOCKS_PROXY_HOST, socksProxyHost);
      properties.put(ProxyConfig.SOCKS_PROXY_PORT, socksProxyPort);

      DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);
      proxyNoAuth.checkExceptions();
      Assert.assertTrue(proxyNoAuth.interceptedConnection());
    }
  }

  @Test
  public void testProxyAuth() throws Exception {
    final InetSocketAddress destinationAddress = new InetSocketAddress("localhost", sabotNode.getPort());

    try(DremioSocks5ProxyServer proxyWithAuth = new DremioSocks5ProxyServer(destinationAddress, SOCKS_PROXY_USERNAME, SOCKS_PROXY_PASSWORD)){
      final String socksProxyAuthHost = proxyWithAuth.address().getHostName();
      final String socksProxyAuthPort = String.valueOf(proxyWithAuth.address().getPort());

      Properties properties = JdbcAssert.getDefaultProperties();
      properties.put(ProxyConfig.SOCKS_PROXY_HOST, socksProxyAuthHost);
      properties.put(ProxyConfig.SOCKS_PROXY_PORT, socksProxyAuthPort);
      properties.put(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USERNAME);
      properties.put(ProxyConfig.SOCKS_PROXY_PASSWORD, SOCKS_PROXY_PASSWORD);

      DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);

      proxyWithAuth.checkExceptions();
      Assert.assertTrue(proxyWithAuth.interceptedConnection());
    }
  }

  @Test
  public void testProxyWrongPassword() {
    final InetSocketAddress destinationAddress = new InetSocketAddress("localhost", sabotNode.getPort());
    try (DremioSocks5ProxyServer proxyWithAuth = new DremioSocks5ProxyServer(destinationAddress, SOCKS_PROXY_USERNAME, SOCKS_PROXY_PASSWORD)) {
      final String socksProxyAuthHost = proxyWithAuth.address().getHostName();
      final String socksProxyAuthPort = String.valueOf(proxyWithAuth.address().getPort());

      Properties properties = JdbcAssert.getDefaultProperties();
      properties.put(ProxyConfig.SOCKS_PROXY_HOST, socksProxyAuthHost);
      properties.put(ProxyConfig.SOCKS_PROXY_PORT, socksProxyAuthPort);
      properties.put(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USERNAME);
      properties.put(ProxyConfig.SOCKS_PROXY_PASSWORD, "gnarly");

      Exception exception = Assert.assertThrows(SQLException.class, () -> {
        DriverManager.getConnection(sabotNode.getJDBCConnectionString(), properties);
      });

      String expectedMessage = "ProxyConnectException";
      String actualMessage = exception.getMessage();

      Assert.assertTrue(actualMessage.contains(expectedMessage));

      proxyWithAuth.checkExceptions();
    }
  }
}
