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
package com.dremio.plugins.async.utils;

import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.http.BufferBasedCompletionHandler;
import com.dremio.io.ExponentialBackoff;

import io.netty.buffer.ByteBuf;

/**
 * Utility class that does read with retry
 */
public class AsyncReadWithRetry {
    private static final Logger logger = LoggerFactory.getLogger(AsyncReadWithRetry.class);
    private static final int MAX_RETRIES = 10;

    public AsyncReadWithRetry() {

    }

    public CompletableFuture<Void> read(AsyncHttpClient asyncHttpClient,
                                               Function<Void, Request> requestBuilderFunction,
                                               MetricsLogger metrics,
                                               Path path,
                                               String threadName,
                                               ByteBuf dst,
                                               int dstOffset,
                                               int retryAttemptNum,
                                               ExponentialBackoff backoff) {

      metrics.startTimer("total");

      if (asyncHttpClient.isClosed()) {
        throw new IllegalStateException("AsyncHttpClient is closed");
      }

      Request req = requestBuilderFunction.apply(null);

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
          backoff.backoffWait(retryAttemptNum);

          metrics.endTimer("backoffwait-" + retryAttemptNum);

          metrics.endTimer("total");
          metrics.logAllMetrics();
          return read(asyncHttpClient, requestBuilderFunction, metrics, path, threadName, dst, dstOffset, retryAttemptNum + 1, backoff);
        }).thenCompose(Function.identity());
    }
}
