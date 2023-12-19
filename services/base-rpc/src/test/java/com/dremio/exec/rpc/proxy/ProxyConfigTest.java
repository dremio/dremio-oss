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
package com.dremio.exec.rpc.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

import org.junit.Test;

public class ProxyConfigTest {
  public static final String SOCKS_PROXY_PORT = "1080";
  public static final String SOCKS_PROXY_HOST = "example.com";
  public static final String SOCKS_PROXY_USER = "user";
  public static final String SOCKS_PROXY_PASSWORD = "123";

  @Test
  public void testCreateConfigWithNullProperties() {
    Properties properties = new Properties();
    Optional<ProxyConfig> proxyConfig = ProxyConfig.of(properties);
    assertFalse(proxyConfig.isPresent());
  }

  @Test
  public void testCreateConfigWithOnlyProxyPort() {
    Properties properties = new Properties();
    properties.setProperty("random_prop", "random_value");
    properties.setProperty(ProxyConfig.SOCKS_PROXY_PORT, SOCKS_PROXY_PORT);

    Optional<ProxyConfig> proxyConfig = ProxyConfig.of(properties);
    assertFalse(proxyConfig.isPresent());
  }

  @Test
  public void testCreateConfigWithOnlyProxyHost() {
    Properties properties = new Properties();
    properties.setProperty("random_prop", "random_value");
    properties.setProperty(ProxyConfig.SOCKS_PROXY_HOST, SOCKS_PROXY_HOST);

    Optional<ProxyConfig> proxyConfig = ProxyConfig.of(properties);
    assertTrue(proxyConfig.isPresent());

    InetSocketAddress expectedAddress = new InetSocketAddress(SOCKS_PROXY_HOST, Integer.parseInt(SOCKS_PROXY_PORT));

    {
      InetSocketAddress actualAddress = proxyConfig.map(ProxyConfig::getProxyAddress).orElse(null);
      assertEquals(expectedAddress, actualAddress);
    }

    {
      DremioSocks5ProxyHandler actualHandler = proxyConfig.map(ProxyConfig::createProxyHandler).orElse(null);
      assertNotNull(actualHandler);
      assertEquals("none", actualHandler.authScheme());
      assertEquals(expectedAddress, actualHandler.proxyAddress());
    }
  }

  @Test
  public void testCreateConfigWithCredentials() {
    Properties properties = new Properties();
    properties.setProperty(ProxyConfig.SOCKS_PROXY_HOST, SOCKS_PROXY_HOST);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USER);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_PASSWORD, SOCKS_PROXY_PASSWORD);

    Optional<ProxyConfig> proxyConfig = ProxyConfig.of(properties);
    InetSocketAddress expectedAddress = new InetSocketAddress(SOCKS_PROXY_HOST, Integer.parseInt(SOCKS_PROXY_PORT));
    {
      DremioSocks5ProxyHandler actualHandler = proxyConfig.map(ProxyConfig::createProxyHandler).orElse(null);

      assertNotNull(actualHandler);
      assertEquals("password", actualHandler.authScheme());
      assertEquals(expectedAddress, actualHandler.proxyAddress());
    }
  }

  @Test
  public void testMissingPassword() {
    Properties properties = new Properties();
    properties.setProperty(ProxyConfig.SOCKS_PROXY_HOST, SOCKS_PROXY_HOST);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_PORT, SOCKS_PROXY_PORT);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_USERNAME, SOCKS_PROXY_USER);

    Optional<ProxyConfig> proxyConfig = ProxyConfig.of(properties);
    {
      assertThrows(NoSuchElementException.class, () -> proxyConfig.ifPresent(ProxyConfig::createProxyHandler));
    }
  }

  @Test
  public void testOutOfRangePort() {
    Properties properties = new Properties();
    properties.setProperty(ProxyConfig.SOCKS_PROXY_HOST, SOCKS_PROXY_HOST);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_PORT, "-1");

    assertThrows(IllegalArgumentException.class, () -> ProxyConfig.of(properties));
  }

  @Test
  public void testNonNumberPort() {
    Properties properties = new Properties();
    properties.setProperty(ProxyConfig.SOCKS_PROXY_HOST, SOCKS_PROXY_HOST);
    properties.setProperty(ProxyConfig.SOCKS_PROXY_PORT, "not a number");

    assertThrows(IllegalArgumentException.class, () -> ProxyConfig.of(properties));
  }

}
