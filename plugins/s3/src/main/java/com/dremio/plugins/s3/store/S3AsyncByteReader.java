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

import static com.amazonaws.services.s3.internal.Constants.REQUESTER_PAYS;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.amazonaws.services.s3.internal.Constants;
import com.dremio.common.exceptions.UserException;
import com.dremio.io.ReusableAsyncByteReader;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A ByteReader that uses AWS's asynchronous S3 client to read byte ranges. No read ahead is done.
 */
class S3AsyncByteReader extends ReusableAsyncByteReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(S3AsyncByteReader.class);

  private static final AtomicInteger numOutstandingReads = new AtomicInteger(0);
  private final S3AsyncClient client;
  private final String bucket;
  private final String path;
  private final Instant instant;
  private final boolean requesterPays;
  private final String threadName;
  private static final int MAX_RETRIES = 10;
  private final boolean ssecEnabled;
  private final String ssecKey;
  private final boolean shouldCheckTimestamp;

  public S3AsyncByteReader(S3AsyncClient client, String bucket, String path,
                           String version, boolean requesterPays,
                           boolean ssecUsed, String sseCustomerKey, boolean shouldCheckTimestamp) {
    super();
    this.client = client;
    this.bucket = bucket;
    this.path = path;
    long mtime = Long.parseLong(version);
    this.instant = (mtime != 0) ? Instant.ofEpochMilli(mtime) : null;
    this.requesterPays = requesterPays;
    this.threadName = Thread.currentThread().getName();
    this.ssecEnabled = ssecUsed;
    this.ssecKey = sseCustomerKey;
    this.shouldCheckTimestamp = shouldCheckTimestamp;
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    if(len == 0) {
      throw new IllegalArgumentException("Empty reads not allowed.");
    }
    logger.debug("[{}] Submitted request to read from s3 bucket {}, path {} at starting offset {} for length {} ", threadName, bucket, path, offset, len);
    return asyncReadWithRetry(offset, dst, dstOffset, len, 1);
  }

  private CompletableFuture<Void> asyncReadWithRetry(long offset, ByteBuf dst, int dstOffset, int len, int retryAttemptNum) {
    dst.writerIndex(dstOffset);
    Stopwatch w = Stopwatch.createStarted();
    numOutstandingReads.incrementAndGet();
    final GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
            .bucket(bucket)
            .key(path)
            .range(range(offset, len));
    if (instant != null && shouldCheckTimestamp) {
      requestBuilder.ifUnmodifiedSince(instant);
    }
    if (requesterPays) {
      requestBuilder.requestPayer(REQUESTER_PAYS);
    }
    if (ssecEnabled) {
      requestBuilder.sseCustomerAlgorithm("AES256");
      requestBuilder.sseCustomerKey(ssecKey);
    }

    CompletableFuture<Void> future = client.getObject(
            requestBuilder.build(),
            new ByteRangeReader(dst, dstOffset));

    return future.whenComplete((response, throwable) -> {
      numOutstandingReads.decrementAndGet();
      if (throwable == null) {
        logger.debug("Finished read of {}.{} for range {}..{} in {}ms.", bucket, path, offset, offset + len, w.elapsed(TimeUnit.MILLISECONDS));
      }
    }).thenAccept(response -> {
    }).thenApply(CompletableFuture::completedFuture).exceptionally(throwable -> {
      boolean retryRequest = false;
      if (throwable.getCause() instanceof NoSuchKeyException) {
        logger.debug("[{}] Request for bucket {}, path {} failed as requested file is not present, took {} ms", threadName,
                bucket, path, w.elapsed(TimeUnit.MILLISECONDS));
        throw new CompletionException(
                new FileNotFoundException("File not found " + path));
      }

      if (throwable.getCause() instanceof S3Exception) {
        int statusCode = ((S3Exception)throwable.getCause()).statusCode();
        switch (statusCode) {
          case Constants.FAILED_PRECONDITION_STATUS_CODE:
            logger.info("[{}] Request for bucket {}, path {} failed as requested version of file not present, took {} ms", threadName,
                    bucket, path, w.elapsed(TimeUnit.MILLISECONDS));
            throw new CompletionException(
                    new FileNotFoundException("Version of file changed " + path));
          case Constants.BUCKET_ACCESS_FORBIDDEN_STATUS_CODE:
            logger.info("[{}] Request for bucket {}, path {} failed as access was denied, took {} ms", threadName,
                    bucket, path, w.elapsed(TimeUnit.MILLISECONDS));
            throw UserException.permissionError(throwable)
                    .message(S3FileSystem.S3_PERMISSION_ERROR_MSG)
                    .build(logger);
          case 500:
          case 503:
            //Retry for Internal Server Error with 500 and 503 status code
            retryRequest = true;
            break;
          default:
            logger.error("[{}] Request for bucket {}, path {} failed with code {}. Failing read, took {} ms", threadName, bucket, path,
                    statusCode, w.elapsed(TimeUnit.MILLISECONDS));
            throw new CompletionException(throwable);
        }
      }
      if (retryRequest || throwable.getCause() instanceof SdkException) {
        if (retryAttemptNum > MAX_RETRIES) {
          throw new CompletionException(throwable);
        }
        logger.warn("Retrying S3Async operation, exception was: {}", throwable.getLocalizedMessage());
        return asyncReadWithRetry(offset, dst, dstOffset, len, retryAttemptNum + 1);
      } else {
        throw new CompletionException(throwable);
      }
    }).thenCompose(Function.identity());
  }


  /**
   * Used to read a single byte range.
   */
  private static class ByteRangeReader implements AsyncResponseTransformer<GetObjectResponse, Void>{

    private int curOffset;
    private final ByteBuf dst;
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private GetObjectResponse response;

    public ByteRangeReader(ByteBuf dst, int dstOffset) {
      super();
      this.dst = dst;
      this.curOffset = dstOffset;
    }

    @Override
    public CompletableFuture<Void> prepare() {
      return future;
    }

    @Override
    public void onResponse(GetObjectResponse response) {
      this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
      publisher.subscribe(new Subscriber<ByteBuffer>() {

        @Override
        public void onSubscribe(Subscription s) {
          s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer b) {
          int len = b.remaining();
          dst.setBytes(curOffset, b);
          curOffset += len;
        }

        @Override
        public void onError(Throwable error) {
          future.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
          future.complete(null);
        }
      });
    }

    @Override
    public void exceptionOccurred(Throwable error) {
      future.completeExceptionally(error);
    }
  }

  /**
   * Generate a standard byte range header string.
   * @param start The first bytes to read.
   * @param len The number of bytes to read
   */
  protected static String range(long start, long len) {
    // https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
    // According to spec, the bytes should be inclusive bounded, thus inclusion of -1 to end boundar.
    return String.format("bytes=%d-%d",start, start + len - 1);
  }
}
