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

package com.dremio.plugins.s3.store;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

/**
 * Apache HTTP Connection Utility that supports aws sdk 2.X
 */
final class ApacheHttpConnectionUtil {
  private static final Logger logger = LoggerFactory.getLogger(ApacheHttpConnectionUtil.class);

  private ApacheHttpConnectionUtil() {
  }

  public static SdkHttpClient.Builder<?> initConnectionSettings(Configuration conf) {
    final ApacheHttpClient.Builder httpBuilder = ApacheHttpClient.builder();
    httpBuilder.maxConnections(intOption(conf, Constants.MAXIMUM_CONNECTIONS, Constants.DEFAULT_MAXIMUM_CONNECTIONS, 1));
    httpBuilder.connectionTimeout(
            Duration.ofSeconds(intOption(conf, Constants.ESTABLISH_TIMEOUT, Constants.DEFAULT_ESTABLISH_TIMEOUT, 0)));
    httpBuilder.socketTimeout(
            Duration.ofSeconds(intOption(conf, Constants.SOCKET_TIMEOUT, Constants.DEFAULT_SOCKET_TIMEOUT, 0)));
    httpBuilder.proxyConfiguration(initProxySupport(conf));

    return httpBuilder;
  }

  private static ProxyConfiguration initProxySupport(Configuration conf) throws IllegalArgumentException {
    final ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

    final String proxyHost = conf.getTrimmed(Constants.PROXY_HOST, "");
    int proxyPort = conf.getInt(Constants.PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      if (proxyPort < 0) {
        if (conf.getBoolean(Constants.SECURE_CONNECTIONS, Constants.DEFAULT_SECURE_CONNECTIONS)) {
          proxyPort = 443;
        } else {
          proxyPort = 80;
        }
      }

      builder.endpoint(URI.create(proxyHost + ":" + proxyPort));

      try {
        final String proxyUsername = lookupPassword(conf, Constants.PROXY_USERNAME);
        final String proxyPassword = lookupPassword(conf, Constants.PROXY_PASSWORD);
        if ((proxyUsername == null) != (proxyPassword == null)) {
          throw new IllegalArgumentException(String.format("Proxy error: %s or %s set without the other.",
                  Constants.PROXY_USERNAME, Constants.PROXY_PASSWORD));
        }

        builder.username(proxyUsername);
        builder.password(proxyPassword);
        builder.ntlmDomain(conf.getTrimmed(Constants.PROXY_DOMAIN));
        builder.ntlmWorkstation(conf.getTrimmed(Constants.PROXY_WORKSTATION));
      } catch (IOException e) {
        throw UserException.sourceInBadState(e).buildSilently();
      }
    } else if (proxyPort >= 0) {
      throw new IllegalArgumentException(String.format("Proxy error: %s set without %s",
              Constants.PROXY_HOST, Constants.PROXY_PORT));
    }

    return builder.build();
  }

  private static int intOption(Configuration conf, String key, int defVal, int min) {
    final int v = conf.getInt(key, defVal);
    logger.debug("For key {} -> configured value is {} and default value is {} ", key, v, defVal);
    Preconditions.checkArgument(v >= min, "Value of %s: %s is below the minimum value %s", key, v, min);
    return v;
  }

  private static String lookupPassword(Configuration conf, String key) throws IOException {
    try {
      final char[] pass = conf.getPassword(key);
      return pass != null ? (new String(pass)).trim() : null;
    } catch (IOException ioe) {
      throw new IOException("Cannot find password option " + key, ioe);
    }
  }
}
