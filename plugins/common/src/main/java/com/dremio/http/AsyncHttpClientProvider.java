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

package com.dremio.http;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import java.io.IOException;

import javax.net.ssl.SSLException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.netty.channel.DefaultChannelPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.HashedWheelTimer;

/**
 * Single point provider for AsyncHttpClient connection objects. The connection properties are common for all consumers.
 */
public class AsyncHttpClientProvider {
  private static final Logger logger = LoggerFactory.getLogger(AsyncHttpClientProvider.class);
  public static final int DEFAULT_REQUEST_TIMEOUT = 10_000;
  private static final int TTL = 60_000;
  private static final int DEFAULT_CLEANER_PERIOD = 1_000;
  private static final int MAX_RETRIES = 4;

  /**
   * Returns a singleton instance.
   * @return
   */
  public static AsyncHttpClient getInstance(){
    return SingletonHelper.INSTANCE;
  }

  // bill pugh style lazily initialized singleton. Helper inner is only loaded when getInstance() is invoked.
  private static final class SingletonHelper {
    private static final AsyncHttpClient INSTANCE = newClient();
  }

  private static AsyncHttpClient newClient() {
    logger.info("Initializing common AsyncHttpClient.");
    final HashedWheelTimer poolTimer = new HashedWheelTimer();
    final DefaultAsyncHttpClientConfig.Builder configBuilder = config()
      .setThreadPoolName("dremio-common-asynchttpclient")
      .setChannelPool(new DefaultChannelPool(TTL, TTL, poolTimer, DEFAULT_CLEANER_PERIOD))
      .setRequestTimeout(DEFAULT_REQUEST_TIMEOUT)
      .setPooledConnectionIdleTimeout(TTL)
      .setResponseBodyPartFactory(AsyncHttpClientConfig.ResponseBodyPartFactory.LAZY)
      .setMaxRequestRetry(MAX_RETRIES);

    try {
      configBuilder.setSslContext(SslContextBuilder.forClient().build());
    } catch (SSLException e) {
      logger.error("Error while setting ssl context in Async Client", e);
    }

    poolTimer.start();
    final AsyncHttpClient client = asyncHttpClient(configBuilder.build());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Closing common AsyncHttpClient.");
        AutoCloseables.close(IOException.class, poolTimer::stop, client);
      } catch (IOException e) {
        logger.error("Error while closing AsyncHttpClient instance", e);
      }
    }));
    return client;
  }
}
