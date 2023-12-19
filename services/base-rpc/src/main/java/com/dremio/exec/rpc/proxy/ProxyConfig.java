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

import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

public final class ProxyConfig {

  public static final String SOCKS_PROXY_HOST = "socksProxyHost";
  public static String SOCKS_PROXY_PORT = "socksProxyPort";
  public static String SOCKS_PROXY_USERNAME = "socksProxyUsername";
  public static String SOCKS_PROXY_PASSWORD = "socksProxyPassword";

  private static final String DEFAULT_PORT = "1080";

  // Proxy related connection properties
  private final String socksProxyHost;

  private final String socksProxyPort;

  private final Optional<String> socksProxyUsername;

  private final Optional<String> socksProxyPassword;

  private final InetSocketAddress proxyAddress;

  /**
   * The {@code ProxyConfig} class represents the configuration for a proxy connection.
   * It provides methods to access and validate proxy settings, as well as create a Socks5 proxy handler.
   */
  private ProxyConfig (Properties props) {

    this.socksProxyHost = getProxyProperty(SOCKS_PROXY_HOST, props);
    this.socksProxyPort = Optional.ofNullable(getProxyProperty(SOCKS_PROXY_PORT, props)).orElse(DEFAULT_PORT);
    this.socksProxyUsername = getOptionalProperty(SOCKS_PROXY_USERNAME, props);
    this.socksProxyPassword = getOptionalProperty(SOCKS_PROXY_PASSWORD, props);
    this.proxyAddress = new InetSocketAddress(socksProxyHost, Integer.parseInt(socksProxyPort));
  }

  private String getProxyProperty(String key, Properties props) {
    return props.getProperty(key);
  }

  private Optional<String> getOptionalProperty(String key, Properties props) {
    return Optional.ofNullable(props.getProperty(key));
  }

  public InetSocketAddress getProxyAddress() {
    return proxyAddress;
  }

  /**
   * Creates a Socks5 proxy handler based on the configured proxy settings.
   *
   * @return the Socks5 proxy handler
   * @throws NoSuchElementException if the proxy requires authentication but the password is not provided
   */
  public DremioSocks5ProxyHandler createProxyHandler() {
    try {
      return socksProxyUsername
        .map(user -> new DremioSocks5ProxyHandler(proxyAddress, user, socksProxyPassword.get()))
        .orElse(new DremioSocks5ProxyHandler(proxyAddress));
    } catch (NoSuchElementException nse) {
      throw new NoSuchElementException("Please provide both socksProxyUsername and socksProxyPassword if your proxy requires authentication");
    }
  }

  public static Optional<ProxyConfig> of(Properties props) {
    String strPort = Optional.ofNullable(props.getProperty(SOCKS_PROXY_PORT)).orElse(DEFAULT_PORT);
    int port = validatePort(strPort);
    props.setProperty(SOCKS_PROXY_PORT, String.valueOf(port));

    return Optional.of(props)
      .filter(ProxyConfig::hasRequiredProperties)
      .map(ProxyConfig::new);
  }

  private static Integer validatePort(String strPort) {
    final int port;
    try {
      port = Integer.parseInt(strPort);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Please provide a parseable integer value for socksProxyPort");
    }

    if (port <= 0 || port > 65535) {
      throw new IllegalArgumentException("Please set socksProxyPort to a valid port number when socksProxyHost is set.");
    }

    return port;
  }

  private static boolean hasRequiredProperties(Properties props) {
    return props.containsKey(SOCKS_PROXY_HOST);
  }

}
