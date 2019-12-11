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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.internal.Constants;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.io.AsyncByteReader;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * The S3 async APIs are unstable. This is a replacement to use a wrapper around the sync APIs to
 * create an async API.
 * <p>
 * This is the workaround suggested in https://github.com/aws/aws-sdk-java-v2/issues/1122
 */
class S3AsyncByteReaderUsingSyncClient implements AsyncByteReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(S3AsyncByteReaderUsingSyncClient.class);
  private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("s3-read-"));

  private final S3Client s3;
  private final String bucket;
  private final String path;
  private final Instant instant;

  S3AsyncByteReaderUsingSyncClient(S3Client s3, String bucket, String path, String version) {
    this.s3 = s3;
    this.bucket = bucket;
    this.path = path;
    this.instant = Instant.ofEpochMilli(Long.parseLong(version));
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dstBuf, int dstOffset, int len) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    S3SyncReadObject readRequest = new S3SyncReadObject(offset, len, dstBuf, dstOffset, future);
    logger.debug("Submitted request to queue for bucket {}, path {} for {}", bucket, path, S3AsyncByteReader.range(offset, len));
    threadPool.submit(readRequest);
    return future;
  }

  /**
   * Scaffolding class to allow easy retries of an operation.
   */
  static class RetryableInvoker {
    private final int maxRetries;
    RetryableInvoker(int maxRetries) {
      this.maxRetries = maxRetries;
    }

    <T> T invoke(Callable<T> operation) throws Exception {
      int retryCount = 0;
      while (true) {
        try {
          return operation.call();
        } catch (SdkBaseException | IOException | RetryableException e) {
          if (retryCount >= maxRetries) {
            throw e;
          }

          logger.warn("Retrying S3Async operation, exception was: {}", e.getLocalizedMessage());
          ++retryCount;
        }
      }
    }
  }

  class S3SyncReadObject implements Runnable {
    private final ByteBuf byteBuf;
    private final int dstOffset;
    private final long offset;
    private final int len;
    private final CompletableFuture<Void> future;
    private final RetryableInvoker invoker;

    S3SyncReadObject(long offset, int len, ByteBuf byteBuf, int dstOffset, CompletableFuture<Void> future) {
      this.offset = offset;
      this.len = len;
      this.byteBuf = byteBuf;
      this.dstOffset = dstOffset;
      this.future = future;

      // Imitate the S3AFileSystem retry logic. See
      // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/Invoker.java
      // which is created with
      // https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/retry/RetryPolicies.java#L63
      this.invoker = new RetryableInvoker(1);
    }

    @Override
    public void run() {
      final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
        .bucket(bucket)
        .key(path)
        .range(S3AsyncByteReader.range(offset, len))
        .ifUnmodifiedSince(instant);
      final GetObjectRequest request = requestBuilder.build();
      final Stopwatch watch = Stopwatch.createStarted();

      try {
        final ResponseBytes<GetObjectResponse> responseBytes = invoker.invoke(() -> s3.getObjectAsBytes(request));
        byteBuf.setBytes(dstOffset, responseBytes.asInputStream(), len);
        logger.debug("Completed request for bucket {}, path {} for {}, took {} ms", bucket, path, request.range(),
          watch.elapsed(TimeUnit.MILLISECONDS));
        future.complete(null);
      } catch (S3Exception s3e) {
        Exception exception = s3e;
        if (s3e.statusCode() == Constants.FAILED_PRECONDITION_STATUS_CODE) {
          logger.info("Request for bucket {}, path {} failed as requested version of file not present, took {} ms",
            bucket, path, watch.elapsed(TimeUnit.MILLISECONDS));
          exception = new FileNotFoundException("Version of file changed " + path);
        } else {
          logger.error("Request for bucket {}, path {} failed with code {}. Failing read, took {} ms", bucket, path,
            s3e.statusCode(), watch.elapsed(TimeUnit.MILLISECONDS));
        }

        future.completeExceptionally(exception);
      } catch (Exception e) {
        logger.error("Failed request for bucket {}, path {} for {}, took {} ms", bucket, path, request.range(),
          watch.elapsed(TimeUnit.MILLISECONDS), e);
        future.completeExceptionally(e);
      }
    }
  }
}
