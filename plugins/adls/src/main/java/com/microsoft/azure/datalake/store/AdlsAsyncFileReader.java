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
package com.microsoft.azure.datalake.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;
import org.asynchttpclient.netty.LazyResponseBodyPart;
import org.asynchttpclient.util.HttpConstants;

import com.dremio.exec.store.dfs.async.AsyncByteReader;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;

/**
 * Reads file content from ADLS Gen 1 asynchronously.
 */
public class AdlsAsyncFileReader implements AsyncByteReader {
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

  private final ADLStoreClient client;
  private final AsyncHttpClient asyncHttpClient;
  private final String path;
  private final String sessionId = UUID.randomUUID().toString();

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
      }
      return super.onStatusReceived(status);
    }

    @Override
    public AsyncHandler.State onBodyPartReceived(HttpResponseBodyPart content) throws Exception {
      if (isErrorResponse) {
        return super.onBodyPartReceived(content);
      }

      // Do not accumulate the content in the super class if it's a valid response.
      // Just stream it to the output buffer.
      if (content instanceof LazyResponseBodyPart) {
        // Avoid intermediate transformation of ByteBuf to ByteBuffer by using getBuf(), which
        // is implementation-specific on LazyResponseBodyPart.
        outputBuffer.writeBytes(((LazyResponseBodyPart) content).getBuf());
      } else {
        outputBuffer.writeBytes(content.getBodyByteBuffer());
      }
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

  public AdlsAsyncFileReader(ADLStoreClient adlStoreClient, AsyncHttpClient asyncClient, String path) {
    this.client = adlStoreClient;
    this.asyncHttpClient = asyncClient;
    this.path = path;
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    final String clientRequestId = UUID.randomUUID().toString();
    int capacityAtOffset = dst.capacity() - dstOffset;
    if (capacityAtOffset < len) {
      logger.debug("Buffer has {} bytes remaining. Attempted to write at offset {} with {} bytes", capacityAtOffset, dstOffset, len);
    }

    final CompletableFuture<String> future = CompletableFuture.supplyAsync( () -> {
      try {
        return client.getAccessToken();
      } catch (IOException ex) {
        throw new CompletionException(ex);
      }
    });

    return future.thenCompose((authToken) -> {
      final AdlsRequestBuilder builder = new AdlsRequestBuilder(client, authToken)
        .setOp(Operation.OPEN)
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

      dst.writerIndex(dstOffset);
      logger.debug("Sending request with clientRequestId: {}", clientRequestId);
      return asyncHttpClient.executeRequest(builder.build(), new AdlsResponseProcessor(dst))
        .toCompletableFuture()
        .whenComplete((response, throwable) -> logger.debug("Request completed for clientRequestId: {}", clientRequestId))
        .thenAccept(response -> {}); // Discard the response, which has already been handled by AdlsResponseProcessor.
    });
  }

  @Override
  public List<ReaderStat> getStats() {
    return new ArrayList<>();
  }
}
