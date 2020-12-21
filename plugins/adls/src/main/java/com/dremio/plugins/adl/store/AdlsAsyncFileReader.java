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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;

import com.dremio.io.ExponentialBackoff;
import com.dremio.io.ReusableAsyncByteReader;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.microsoft.azure.datalake.store.ADLSClient;
import com.microsoft.azure.datalake.store.OperationResponse;

import io.netty.buffer.ByteBuf;

/**
 * Reads file content from ADLS Gen 1 asynchronously.
 */
public class AdlsAsyncFileReader extends ReusableAsyncByteReader implements ExponentialBackoff {
  static class RemoteException {
    private String exception;
    private String message;
    private String javaClassName;

    public String getException() {
      return exception;
    }

    public String getMessage() {
      return message;
    }

    public String getJavaClassName() {
      return javaClassName;
    }

    @JsonProperty("RemoteException")
    private void unpackRemoteException(Map<String, String> remoteException) {
      // JSON error response is as follows:
      // { "RemoteException" : {
      //   "exception" : ...,
      //   "message"   : ...,
      //   "javaClassName" : ... }
      // }

      // We need to unpack the RemoteException property to get the meaningful attributes.
      exception = remoteException.get("exception");
      message = remoteException.get("message");
      javaClassName = remoteException.get("javaClassName");
    }
  }

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AdlsAsyncFileReader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY, true);

  private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
  private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;
  private static final int MAX_RETRIES = 4;

  private final ADLSClient client;
  private final AsyncHttpClient asyncHttpClient;
  private final String path;
  private final String sessionId = UUID.randomUUID().toString();
  private final DremioAdlFileSystem fs;
  private final Long cachedVersion;
  private volatile Long latestVersion;
  private final String threadName;
  private final ExecutorService threadPool;
  private int errCode = 0;

  /**
   * Helper class for processing ADLS responses. This will write to the given output buffer
   * for OK results and throw a detailed exception on failure.
   */
  private class AdlsResponseProcessor extends AsyncCompletionHandlerBase {
    private final ByteBuf outputBuffer;
    private boolean isErrorResponse = false;

    AdlsResponseProcessor(ByteBuf outputBuffer) {
      this.outputBuffer = outputBuffer;
    }

    @Override
    public State onStatusReceived(HttpResponseStatus status) throws Exception {
      // The REST service provides error information as part of the response
      // body when the response code is 400 or greater, and not a 401 (auth error).
      if (status.getStatusCode() >= io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST.code() &&
          status.getStatusCode() != HttpConstants.ResponseStatusCodes.UNAUTHORIZED_401) {
        isErrorResponse = true;
        errCode = status.getStatusCode();
      }
      return super.onStatusReceived(status);
    }

    @Override
    public AsyncHandler.State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
      if (isErrorResponse) {
        return super.onBodyPartReceived(content);
      }

      outputBuffer.writeBytes(content.getBodyByteBuffer());
      return AsyncHandler.State.CONTINUE;
    }

    @Override
    public Response onCompleted(Response response) throws Exception {
      if (isErrorResponse) {
        throwRemoteException(response.getResponseBody());
      }
      return response;
    }

    private void throwRemoteException(String responseString) throws Exception {
      final RemoteException remoteEx = OBJECT_MAPPER.readValue(responseString, RemoteException.class);
      final OperationResponse resp = new OperationResponse();
      resp.remoteExceptionName = remoteEx.getException();
      resp.remoteExceptionMessage = remoteEx.getMessage();
      resp.remoteExceptionJavaClassName = remoteEx.getJavaClassName();

      throw client.getExceptionFromResponse(resp, String.format("Error reading file %s", path));
    }
  }

  public AdlsAsyncFileReader(ADLSClient adlsClient, AsyncHttpClient asyncClient, String path, String cachedVersion,
                             DremioAdlFileSystem fs, ExecutorService threadPool) {
    this.client = adlsClient;
    this.asyncHttpClient = asyncClient;
    this.path = path;
    this.fs = fs;
    this.cachedVersion = Long.parseLong(cachedVersion);
    this.latestVersion = null;
    this.threadName = Thread.currentThread().getName();
    this.threadPool = threadPool;
  }

  @Override
  public int getBaseMillis() {
    return BASE_MILLIS_TO_WAIT;
  }

  @Override
  public int getMaxMillis() {
    return MAX_MILLIS_TO_WAIT;
  }

  @Override
  protected void onClose() {
    latestVersion = null;
  }

  private CompletableFuture<Void> readFullyFuture(long offset, ByteBuf dst, int dstOffset, int len) {
    if (latestVersion > cachedVersion) {
      logger.debug("[{}] File has been modified, metadata refresh is required", threadName);
      final CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new FileNotFoundException("Version of file changed " + path));
      return future;
    }

    final String clientRequestId = UUID.randomUUID().toString();
    final int capacityAtOffset = dst.capacity() - dstOffset;
    if (capacityAtOffset < len) {
      logger.debug("[{}] Buffer has {} bytes remaining. Attempted to write at offset {} with {} bytes", threadName, capacityAtOffset, dstOffset, len);
    }

    final CompletableFuture<String> future = CompletableFuture.supplyAsync( () -> {
      try {
        return client.getAccessToken();
      } catch (IOException ex) {
        throw new CompletionException(ex);
      }
    }, threadPool);

    return future.thenCompose((authToken) -> {
      final ADLSClient.AdlsRequestBuilder builder = new ADLSClient.AdlsRequestBuilder(client.getClient(), authToken, logger)
        .setOp(ADLSClient.getOpenOperation())
        .addQueryParam("read", "true")
        .addQueryParam("filesessionid", sessionId)
        .addHeader("x-ms-client-request-id", clientRequestId)
        .setFilePath(path);

      if (offset > 0) {
        builder.addQueryParam("offset", Long.toString(offset));
      }
      if (len >= 0) {
        builder.addQueryParam("length", Long.toString(len));
      }

      return executeAsyncRequest(dst, dstOffset, clientRequestId, builder, 0);
    });
  }

  private CompletableFuture<Void> executeAsyncRequest(ByteBuf dst, int dstOffset, String clientRequestId,
                                                      ADLSClient.AdlsRequestBuilder builder, int retryAttemptNum) {
    logger.debug("[{}] Sending request with clientRequestId: {}", threadName, clientRequestId);
    dst.writerIndex(dstOffset);
    final Stopwatch watch = Stopwatch.createStarted();
    return asyncHttpClient.executeRequest(builder.build(), new AdlsResponseProcessor(dst))
      .toCompletableFuture()
      .whenComplete((response, throwable) -> {
        if (null == throwable) {
          logger.debug("[{}] Request completed for clientRequestId: {}, took {} ms", threadName, clientRequestId,
            watch.elapsed(TimeUnit.MILLISECONDS));
        } else if (retryAttemptNum < MAX_RETRIES) {
          logger.info("[{}] Retry #{}, request failed with {} clientRequestId: {}, took {} ms", threadName, retryAttemptNum + 1, errCode,
            clientRequestId, watch.elapsed(TimeUnit.MILLISECONDS), throwable);
        } else {
          logger.error("[{}] Request failed with {}, for clientRequestId: {}, took {} ms", threadName, errCode,
            clientRequestId, watch.elapsed(TimeUnit.MILLISECONDS), throwable);
        }
      })
      .thenAccept(response -> {}) // Discard the response, which has already been handled by AdlsResponseProcessor.
      .thenApply(CompletableFuture::completedFuture)
      .exceptionally(throwable -> {
        if (retryAttemptNum > MAX_RETRIES) {
          final CompletableFuture<Void> errorFuture = new CompletableFuture<>();
          errorFuture.completeExceptionally(throwable);
          return errorFuture;
        }

        backoffWait(retryAttemptNum);

        // Reset the index of the writer for the retry.
        dst.writerIndex(dstOffset);
        return executeAsyncRequest(dst, dstOffset, clientRequestId, builder, retryAttemptNum + 1);
      }).thenCompose(Function.identity());
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    if (latestVersion != null) {
      return readFullyFuture(offset, dst, dstOffset, len);
    }

    final CompletableFuture<Void> getStatusFuture = CompletableFuture.runAsync(() -> {
      try {
        if (latestVersion == null) {
          synchronized (AdlsAsyncFileReader.this) {
            if (latestVersion == null) {
              latestVersion = fs.getFileStatus(new Path(path)).getModificationTime();
            }
          }
        }
      } catch (IOException ex) {
        throw new CompletionException(ex);
      }
    }, threadPool);

    return getStatusFuture.thenCompose(Void -> readFullyFuture(offset, dst, dstOffset, len));
  }
}
