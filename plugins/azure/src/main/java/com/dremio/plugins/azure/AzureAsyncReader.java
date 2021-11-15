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

package com.dremio.plugins.azure;

import static com.dremio.common.utils.PathUtils.removeLeadingSlash;

import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.io.ExponentialBackoff;
import com.dremio.io.ReusableAsyncByteReader;
import com.dremio.plugins.async.utils.AsyncReadWithRetry;
import com.dremio.plugins.async.utils.MetricsLogger;
import com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;

import io.netty.buffer.ByteBuf;

/**
 * Direct HTTP client, for doing azure storage operations
 */
public class AzureAsyncReader extends ReusableAsyncByteReader implements AutoCloseable {
  private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
  private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;

  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncReader.class);
  private final AsyncHttpClient asyncHttpClient;
  private final AzureAuthTokenProvider authProvider;
  private final Path path;
  private final String version;
  private final String url;
  private final String threadName;
  private final AsyncReadWithRetry asyncReaderWithRetry;
  private final ExponentialBackoff backoff = new ExponentialBackoff() {
    @Override public int getBaseMillis() { return BASE_MILLIS_TO_WAIT; }
    @Override public int getMaxMillis() { return MAX_MILLIS_TO_WAIT; }
  };

  public AzureAsyncReader(final String azureEndpoint,
                          final String accountName,
                          final Path path,
                          final AzureAuthTokenProvider authProvider,
                          final String version,
                          final boolean isSecure,
                          final AsyncHttpClient asyncHttpClient) {
    this(azureEndpoint, accountName, path, authProvider, version, isSecure, asyncHttpClient, new AsyncReadWithRetry());
  }

  public AzureAsyncReader(final String azureEndpoint,
                          final String accountName,
                          final Path path,
                          final AzureAuthTokenProvider authProvider,
                          final String version,
                          final boolean isSecure,
                          final AsyncHttpClient asyncHttpClient,
                          AsyncReadWithRetry asyncReadWithRetry) {
    this.authProvider = authProvider;
    this.path = path;
    long mtime = Long.parseLong(version);
    this.version = (mtime != 0) ? AzureAsyncHttpClientUtils.toHttpDateFormat(mtime) : null;
    this.asyncHttpClient = asyncHttpClient;
    final String baseURL = AzureAsyncHttpClientUtils.getBaseEndpointURL(azureEndpoint, accountName, isSecure);
    final String container = DremioHadoopUtils.getContainerName(path);
    final String subPath = removeLeadingSlash(DremioHadoopUtils.pathWithoutContainer(path).toString());
    this.url = String.format("%s/%s/%s", baseURL, container, AzureAsyncHttpClientUtils.encodeUrl(subPath));
    this.threadName = Thread.currentThread().getName();
    this.asyncReaderWithRetry = asyncReadWithRetry;
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    return read(offset, dst, dstOffset, len, 0);
  }

  public CompletableFuture<Void> read(long offset, ByteBuf dst, int dstOffset, long len, int retryAttemptNum) {
    MetricsLogger metrics = getMetricLogger();
    java.util.function.Function<Void, Request> requestBuilderFunction = getRequestBuilderFunction(offset, len, metrics);
    return asyncReaderWithRetry.read(asyncHttpClient, requestBuilderFunction,
            metrics, path, threadName, dst, dstOffset, retryAttemptNum, backoff);
  }

  java.util.function.Function<Void, Request> getRequestBuilderFunction(long offset, long len, MetricsLogger metrics) {
    java.util.function.Function<Void, Request> requestBuilderFunction = (Function<Void, Request>) unused -> {
      long rangeEnd = offset + len - 1L;
      RequestBuilder requestBuilder = AzureAsyncHttpClientUtils.newDefaultRequestBuilder()
              .addHeader("Range", String.format("bytes=%d-%d", offset, rangeEnd))
              .setUrl(url);
      if (version != null) {
        requestBuilder.addHeader("If-Unmodified-Since", version);
      }
      Request req = requestBuilder.build();

      metrics.startTimer("get-authz-header");
      req.getHeaders().add("Authorization", authProvider.getAuthzHeaderValue(req));
      metrics.endTimer("get-authz-header");

      logger.debug("[{}] Req: URL {} {} {}", threadName, req.getUri(), req.getHeaders().get("x-ms-client-request-id"),
              req.getHeaders().get("Range"));
      return req;
    };
    return requestBuilderFunction;
  }

  @Override
  protected void onClose() {
  }

  @VisibleForTesting
  AsyncReadWithRetry getAsyncReaderWithRetry() {
    return asyncReaderWithRetry;
  }

  @VisibleForTesting
  AsyncHttpClient getAsyncHttpClient() {
    return asyncHttpClient;
  }

  @VisibleForTesting
  Path getPath() {
    return path;
  }

  @VisibleForTesting
  String getThreadName() {
    return threadName;
  }

  @VisibleForTesting
  ExponentialBackoff getBackoff() {
    return backoff;
  }

  @VisibleForTesting
  MetricsLogger getMetricLogger() {
    return new MetricsLogger();
  }
}
