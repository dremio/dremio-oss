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

package com.dremio.plugins.azure.utils;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

import javax.net.ssl.SSLException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.netty.channel.DefaultChannelPool;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.DateTimeRfc1123;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.HashedWheelTimer;

/**
 * Prepares new instances of AsyncHttpClient
 */
public final class AzureAsyncHttpClientUtils {
  /**
   * Endpoint, that points to the Azure server API.
   */
  public enum Endpoint {
    DFS, BLOB;

    public String toString() {
      return name().toLowerCase();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncHttpClientUtils.class);
  private static final int DEFAULT_IDLE_TIME = 60_000;
  private static final int DEFAULT_REQUEST_TIMEOUT = 10_000;
  private static final int DEFAULT_CLEANER_PERIOD = 1_000;
  private static final int MAX_RETRIES = 4;
  private static final String AZURE_ENDPOINT = "core.windows.net";
  private static final String XMS_VERSION = "2019-02-02"; // represents version compatibility of the client.
  private static final String USER_AGENT_VAL = String.format("azsdk-java-azure-storage-blob/12.1.0 (%s; %s %s)",
    System.getProperty("java.version"),
    System.getProperty("os.name"),
    System.getProperty("os.version"));

  private AzureAsyncHttpClientUtils() {
    // Not to be instantiated.
  }

  public static AsyncHttpClient newClient(final String accountName,
                                          final boolean isSecure,
                                          final HashedWheelTimer poolTimer) {
    final DefaultAsyncHttpClientConfig.Builder configBuilder = config()
      // TODO: Confirm a new thread pool is not getting created everytime
      .setThreadPoolName(accountName + "-azurestorage-async-client")
      .setChannelPool(new DefaultChannelPool(DEFAULT_IDLE_TIME, -1, poolTimer, DEFAULT_CLEANER_PERIOD))
      .setRequestTimeout(DEFAULT_REQUEST_TIMEOUT)
      .setResponseBodyPartFactory(AsyncHttpClientConfig.ResponseBodyPartFactory.LAZY)
      .setMaxRequestRetry(MAX_RETRIES);

    try {
      if (isSecure) {
        configBuilder.setSslContext(SslContextBuilder.forClient().build());
      }
    } catch (SSLException e) {
      logger.error("Error while setting ssl context in Async Client", e);
    }

    poolTimer.start();
    return asyncHttpClient(configBuilder.build());
  }

  public static String getBaseEndpointURL(final String accountName, final Endpoint endpoint, final boolean isSecure) {
    final String protocol = isSecure ? "https" : "http";
    return String.format("%s://%s.%s.%s", protocol, accountName, endpoint, AZURE_ENDPOINT);
  }

  public static RequestBuilder newDefaultRequestBuilder() {
    return new RequestBuilder(HttpConstants.Methods.GET)
      .addHeader("Date", toHttpDateFormat(System.currentTimeMillis()))
      .addHeader("Content-Length", 0)
      .addHeader("x-ms-version", XMS_VERSION)
      .addHeader("x-ms-client-request-id", UUID.randomUUID().toString())
      .addHeader("User-Agent", USER_AGENT_VAL);
  }

  public static String encodeUrl(String raw) {
    try {
      return URLEncoder.encode(raw, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      logger.warn("Error while enconding " + raw, e);
      return raw;
    }
  }

  public static String toHttpDateFormat(final long timeInMillis) {
    final OffsetDateTime time = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.of("GMT"));
    final DateTimeRfc1123 dateTimeRfc1123 = new DateTimeRfc1123(time);
    return dateTimeRfc1123.toString();
  }
}
