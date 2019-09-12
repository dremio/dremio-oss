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
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.s3.internal.Constants;
import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.io.AsyncByteReader;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.ResponseBytes;
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

  class S3SyncReadObject implements Runnable {
    private final ByteBuf byteBuf;
    private final int dstOffset;
    private final long offset;
    private final int len;
    private final CompletableFuture<Void> future;

    S3SyncReadObject(long offset, int len, ByteBuf byteBuf, int dstOffset, CompletableFuture<Void> future) {
      this.offset = offset;
      this.len = len;
      this.byteBuf = byteBuf;
      this.dstOffset = dstOffset;
      this.future = future;
    }

    @Override
    public void run() {
      GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
        .bucket(bucket)
        .key(path)
        .range(S3AsyncByteReader.range(offset, len))
        .ifUnmodifiedSince(instant);
      GetObjectRequest request = requestBuilder.build();

      try {
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectAsBytes(request);
        byteBuf.setBytes(dstOffset, responseBytes.asInputStream(), len);
        logger.debug("Completed request for bucket {}, path {} for {}", bucket, path, request.range());
        future.complete(null);
      } catch (S3Exception s3e) {
        Exception exception = s3e;
        if (s3e.statusCode() == Constants.FAILED_PRECONDITION_STATUS_CODE) {
          logger.info("Request for bucket {}, path {} failed as requested version of file not present", bucket, path);
          exception = new FileNotFoundException("Version of file changed " + path);
        } else {
          logger.error("Request for bucket {}, path {} failed. Failing read", bucket, path);
        }

        future.completeExceptionally(exception);
      } catch (Exception e) {
        logger.debug("Failed request for bucket {}, path {} for {}", bucket, path, request.range());
        future.completeExceptionally(e);
      }
    }
  }
}
