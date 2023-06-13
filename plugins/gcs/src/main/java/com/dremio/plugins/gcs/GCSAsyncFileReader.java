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
package com.dremio.plugins.gcs;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;

import com.dremio.exec.hadoop.DremioHadoopUtils;
import com.dremio.http.BufferBasedCompletionHandler;
import com.dremio.io.ExponentialBackoff;
import com.dremio.io.ReusableAsyncByteReader;
import com.dremio.plugins.async.utils.AsyncReadWithRetry;
import com.dremio.plugins.async.utils.MetricsLogger;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Function;

import io.netty.buffer.ByteBuf;

/**
 * Reads file content from GCS asynchronously.
 */
class GCSAsyncFileReader extends ReusableAsyncByteReader {
  private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
  private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;

  private final GoogleCredentials credentials;
  private final AsyncHttpClient asyncHttpClient;
  private final String bucket;
  private final String blob;
  private final String httpVersion;
  private final Path path;
  private final String threadName;
  private final AsyncReadWithRetry asyncReaderWithRetry = new AsyncReadWithRetry(throwable -> {
    if (throwable.getMessage().contains("PreconditionFailed")) {
      return AsyncReadWithRetry.Error.PRECONDITION_NOT_MET;
    } else if (throwable.getMessage().contains("PathNotFound")) {
      return AsyncReadWithRetry.Error.PATH_NOT_FOUND;
    } else {
      return AsyncReadWithRetry.Error.UNKNOWN;
    }
  });
  private final ExponentialBackoff backoff = new ExponentialBackoff() {
    @Override public int getBaseMillis() { return BASE_MILLIS_TO_WAIT; }
    @Override public int getMaxMillis() { return MAX_MILLIS_TO_WAIT; }
  };

  public GCSAsyncFileReader(
      AsyncHttpClient asyncClient,
      Path fsPath,
      String cachedVersion,
      GoogleCredentials credentials) {
    this.credentials = credentials;
    this.asyncHttpClient = asyncClient;
    this.blob = DremioHadoopUtils.pathWithoutContainer(fsPath).toString();
    this.bucket = DremioHadoopUtils.getContainerName(fsPath);
    this.httpVersion = convertToHttpDateTime(cachedVersion);
    this.path = fsPath;
    this.threadName = Thread.currentThread().getName();
  }

  // Utility method to convert string representation of a datetime Long value into a HTTP 1.1
  // protocol compliant datetime string representation as follow:
  //      Sun, 06 Nov 1994 08:49:37 GMT  ; RFC 822, updated by RFC 1123
  //      Sunday, 06-Nov-94 08:49:37 GMT ; RFC 850, obsoleted by RFC 1036
  //      Sun Nov  6 08:49:37 1994       ; ANSI C's asctime() format
  // References:
  // HTTP/1.1 protocol specification: https://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3
  // Conditional headers: https://docs.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
  private static String convertToHttpDateTime(String version) {
    if(version == null) {
      return null;
    }
    long unixTimestampMilli = Long.parseLong(version);
    if (unixTimestampMilli == 0) {
      return null;
    }
    ZonedDateTime date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(unixTimestampMilli), ZoneId.of("GMT"));
    String dayOfWeek = date.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
    // DateTimeFormatter does not support commas
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss", Locale.ENGLISH);
    return String.format("%s, %s GMT", dayOfWeek, date.format(formatter));
  }

  @Override
  protected void onClose() {
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    MetricsLogger metrics = new MetricsLogger();
    java.util.function.Function<Void, Request> requestBuilderFunction = getRequestBuilderFunction(offset, len);

    return asyncReaderWithRetry.read(asyncHttpClient, requestBuilderFunction,
            metrics, path, threadName, new BufferBasedCompletionHandler(dst, dstOffset), 0, backoff);
  }

  private java.util.function.Function<Void, Request> getRequestBuilderFunction(long offset, int len) {
    java.util.function.Function<Void, Request> requestBuilderFunction = (Function<Void, Request>) unused -> {
      try {
        String encodedBlob = URLEncoder.encode(blob.substring(1), StandardCharsets.UTF_8.toString()).replace("+", "%20");
        // If-Unmodified-Since is only supported on the xml api.
        //String uri = String.format("https://www.googleapis.com/download/storage/v1/b/%s/o/%s?alt=media", bucket, encodedBlob);
        String uri = String.format("https://storage.googleapis.com/%s/%s", bucket, encodedBlob);

        RequestBuilder requestBuilder = new RequestBuilder()
                .setUri(Uri.create(uri));

        if (httpVersion != null) {
          requestBuilder.addHeader("If-Unmodified-Since", httpVersion);
        }

        credentials.refreshIfExpired();

        for (Entry<String, List<String>> e : credentials.getRequestMetadata().entrySet()) {
          requestBuilder.addHeader(e.getKey(), e.getValue());
        }

        if (len >= 0) {
          requestBuilder.addHeader("range", String.format("bytes=%d-%d", offset, offset + len - 1));
        }

        return requestBuilder.build();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
    return requestBuilderFunction;
  }
}
