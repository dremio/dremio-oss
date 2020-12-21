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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.dremio.io.ReusableAsyncByteReader;
import com.google.common.base.Stopwatch;

import io.netty.buffer.ByteBuf;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * A ByteReader that uses AWS's asynchronous S3 client to read byte ranges. No read ahead is done.
 */
class S3AsyncByteReader extends ReusableAsyncByteReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(S3AsyncByteReader.class);

  private static final AtomicInteger numOutstandingReads = new AtomicInteger(0);
  private final S3AsyncClient client;
  private final String bucket;
  private final String path;

  public S3AsyncByteReader(S3AsyncClient client, String bucket, String path) {
    super();
    this.client = client;
    this.bucket = bucket;
    this.path = path;
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    if(len == 0) {
      throw new IllegalArgumentException("Empty reads not allowed.");
    }
    logger.debug("Starting read of {}.{} for range {}..{}", bucket, path, offset, offset + len);
    Stopwatch w = Stopwatch.createStarted();
    numOutstandingReads.incrementAndGet();
    CompletableFuture<Void> future = client.getObject(
      GetObjectRequest.builder()
        .range(range(offset, len))
        .bucket(bucket)
        .key(path)
        .build(),
      new ByteRangeReader(dst, dstOffset));

    return future.whenComplete((a,b) -> {
      int numOutstanding = numOutstandingReads.decrementAndGet();
      if (b == null) {
        // no exception
        logger.debug("Finished read of {}.{} for range {}..{} in {}ms.", bucket, path, offset, offset + len, w.elapsed(TimeUnit.MILLISECONDS));
        return;
      }

      // exception
      logger.warn("Async read of {}.{} for length {} failed in {}ms when there are {} outstanding reads. Error {}", bucket, path, len, w.elapsed(TimeUnit.MILLISECONDS), numOutstanding, b);
    });
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
