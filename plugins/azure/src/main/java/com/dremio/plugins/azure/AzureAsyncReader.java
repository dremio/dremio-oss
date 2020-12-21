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

import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.io.ExponentialBackoff;
import com.dremio.io.ReusableAsyncByteReader;
import com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils;
import com.dremio.plugins.azure.utils.MetricsLogger;
import com.dremio.plugins.util.ContainerFileSystem;

import io.netty.buffer.ByteBuf;

/**
 * Direct HTTP client, for doing azure storage operations
 */
public class AzureAsyncReader extends ReusableAsyncByteReader implements ExponentialBackoff, AutoCloseable {
  private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
  private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;
  private static final int MAX_RETRIES = 10;

  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncReader.class);
  private final AsyncHttpClient asyncHttpClient;
  private AzureAuthTokenProvider authProvider;
  private final Path path;
  private final String version;
  private final String url;
  private final String threadName;

  public AzureAsyncReader(final String accountName,
                          final Path path,
                          final AzureAuthTokenProvider authProvider,
                          final String version,
                          final boolean isSecure,
                          final AsyncHttpClient asyncHttpClient) {
    this.authProvider = authProvider;
    this.path = path;
    this.version = AzureAsyncHttpClientUtils.toHttpDateFormat(Long.parseLong(version));
    this.asyncHttpClient = asyncHttpClient;
    final String baseURL = AzureAsyncHttpClientUtils.getBaseEndpointURL(accountName, isSecure);
    final String container = ContainerFileSystem.getContainerName(path);
    final String subPath = removeLeadingSlash(ContainerFileSystem.pathWithoutContainer(path).toString());
    this.url = String.format("%s/%s/%s", baseURL, container, AzureAsyncHttpClientUtils.encodeUrl(subPath));
    this.threadName = Thread.currentThread().getName();
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    return read(offset, dst, dstOffset, len, 0);
  }

  public CompletableFuture<Void> read(long offset, ByteBuf dst, int dstOffset, long len, int retryAttemptNum) {
    MetricsLogger metrics = new MetricsLogger();
    metrics.startTimer("total");

    if (asyncHttpClient.isClosed()) {
      throw new IllegalStateException("AsyncHttpClient is closed");
    }

    metrics.startTimer("update-token");
    if (authProvider.checkAndUpdateToken()) {
      metrics.incrementCounter("new-client");
    }

    metrics.endTimer("update-token");
    long rangeEnd = offset + len - 1L;
    Request req = AzureAsyncHttpClientUtils.newDefaultRequestBuilder()
      .addHeader("Range", String.format("bytes=%d-%d", offset, rangeEnd))
      .addHeader("If-Unmodified-Since", version)
      .setUrl(url)
      .build();
    req.getHeaders().add("Authorization", authProvider.getAuthzHeaderValue(req));

    logger.debug("[{}] Req: URL {} {} {}", threadName, req.getUri(), req.getHeaders().get("x-ms-client-request-id"),
      req.getHeaders().get("Range"));

    metrics.startTimer("request");
    dst.writerIndex(dstOffset);
    return asyncHttpClient.executeRequest(req, new BufferBasedCompletionHandler(dst))
      .toCompletableFuture()
      .whenComplete((response, throwable) -> {
        metrics.endTimer("request");
        if (throwable == null) {
          metrics.incrementCounter("success");
        }
      })
      .thenAccept(response -> {
        metrics.endTimer("total");
        metrics.logAllMetrics();
      }) // Discard the response, which has already been handled.
      .thenApply(CompletableFuture::completedFuture)
      .exceptionally(throwable -> {
        metrics.incrementCounter("error");
        logger.error("[{}] Error while executing request", threadName, throwable);

        final CompletableFuture<Void> errorFuture = new CompletableFuture<>();
        if (throwable.getMessage().contains("ConditionNotMet")) {
          errorFuture.completeExceptionally(new FileNotFoundException("Version of file has changed " + path));
          return errorFuture;
        } else if (throwable.getMessage().contains("PathNotFound")) {
          errorFuture.completeExceptionally(new FileNotFoundException("File " + path
            + " not found: " + throwable.getMessage()));
          return errorFuture;
        } else if (retryAttemptNum > MAX_RETRIES) {
          metrics.incrementCounter("retry" + retryAttemptNum);
          logger.error("[{}] Error while reading {}. Operation failing beyond retries.", threadName, path);
          errorFuture.completeExceptionally(throwable);
          return errorFuture;
        }
        metrics.startTimer("backoffwait-" + retryAttemptNum);
        backoffWait(retryAttemptNum);
        metrics.endTimer("backoffwait-" + retryAttemptNum);

        metrics.endTimer("total");
        metrics.logAllMetrics();
        return read(offset, dst, dstOffset, len, retryAttemptNum + 1);
      }).thenCompose(Function.identity());
  }

  @Override
  protected void onClose() {
  }

  @Override
  public int getBaseMillis() {
    return BASE_MILLIS_TO_WAIT;
  }

  @Override
  public int getMaxMillis() {
    return MAX_MILLIS_TO_WAIT;
  }
}
