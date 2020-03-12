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
package com.dremio.plugins.adl.store;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.netty.channel.DefaultChannelPool;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.ADLStoreOptions;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.RefreshTokenBasedTokenProvider;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.HashedWheelTimer;

/**
 * Helper class for constructor and maintaining AsyncHttpClients.
 */
public class AsyncHttpClientManager implements Closeable {
  private static final int DEFAULT_IDLE_TIME = 60000;
  private static final int DEFAULT_CLEANER_PERIOD = 1000;
  private static final int DEFAULT_NUM_RETRY = 4; // Set 4 retry attempts. In the ADLS SDK, the ExponentialBackoffPolicy makes 4 retry attempts by default.

  private AsyncHttpClient asyncHttpClient;
  private ADLStoreClient client;
  private ExecutorService utilityThreadPool;
  private final HashedWheelTimer poolTimer = new HashedWheelTimer();

  public AsyncHttpClientManager(String name, AzureDataLakeConf conf) throws IOException {
    final AccessTokenProvider tokenProvider;
    switch (conf.mode) {
      case CLIENT_KEY:
        tokenProvider = new ClientCredsTokenProvider(conf.clientKeyRefreshUrl,
          conf.clientId, conf.clientKeyPassword);
        break;
      case REFRESH_TOKEN:
        tokenProvider = new RefreshTokenBasedTokenProvider(conf.clientId, conf.refreshTokenSecret);
        break;
      default:
        throw new RuntimeException("Failure creating ADLSg1 connection. Invalid credentials type.");
    }

    final SslContext sslContext = SslContextBuilder
      .forClient()
      .build();

    // Configure our AsyncHttpClient to:
    // - use SSL (required for ADLS)
    // - use connection pooling
    // - generate response bodies lazily (leave them as Netty ByteBufs instead of convert them to byte[]).
    final DefaultAsyncHttpClientConfig.Builder configBuilder = config()
      .setSslContext(sslContext)
      .setThreadPoolName(name + "-adls-async-client")
      .setChannelPool(new DefaultChannelPool(DEFAULT_IDLE_TIME, -1, poolTimer, DEFAULT_CLEANER_PERIOD))
      .setResponseBodyPartFactory(AsyncHttpClientConfig.ResponseBodyPartFactory.LAZY)
      .setMaxRequestRetry(DEFAULT_NUM_RETRY);

    poolTimer.start();

    client = ADLStoreClient.createClient(
      conf.accountName + ".azuredatalakestore.net", tokenProvider);

    client.setOptions(new ADLStoreOptions().enableThrowingRemoteExceptions());
    asyncHttpClient = asyncHttpClient(configBuilder.build());
    utilityThreadPool = Executors.newCachedThreadPool(new NamedThreadFactory("adls-utility"));
  }

  public AsyncHttpClient getAsyncHttpClient() {
    return asyncHttpClient;
  }

  public ADLStoreClient getClient() {
    return client;
  }

  public ExecutorService getUtilityThreadPool() {
    return utilityThreadPool;
  }

  @Override
  public void close() throws IOException {
    AutoCloseables.close(IOException.class, asyncHttpClient, poolTimer::stop, utilityThreadPool::shutdown);
  }
}
