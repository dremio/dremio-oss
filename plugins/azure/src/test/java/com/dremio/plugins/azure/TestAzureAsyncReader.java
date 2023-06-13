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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;

import com.dremio.plugins.async.utils.AsyncReadWithRetry;
import com.dremio.plugins.async.utils.MetricsLogger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * Tests for AzureAsyncReader
 */
public class TestAzureAsyncReader {

  private static final String AZURE_ENDPOINT = "dfs.core.windows.net";

  private static final DateTimeFormatter DATE_RFC1123_FORMATTER = DateTimeFormatter
    .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
    .withZone(ZoneId.of("UTC"))
    .withLocale(Locale.US);

  private final Random random = new Random();

  void verifyTestFileVersionChanged(boolean checkVersion) {
    final String responseBody = "{\"error\":{\"code\":\"ConditionNotMet\",\"message\":\"The condition specified using HTTP " +
      "conditional header(s) is not met.\\nRequestId:89fa17ae-501f-0002-4bf0-168aaa000000\\nTime:2020-04-20T08:49:44.4893649Z\"}}";
    final int responseCode = 412;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode, checkVersion);
    ByteBuf buf = Unpooled.buffer(20);

    try {
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(FileNotFoundException.class, e.getCause().getClass());
      assertTrue(e.getCause().getMessage().contains("Version of file has changed"));
    } finally {
      verify(azureAsyncReader, times(1)).read(eq(0L), eq(20L), any(ChecksumVerifyingCompletionHandler.class), eq(0));
    }
  }

  @Test
  public void testFileVersionChanged() {
    verifyTestFileVersionChanged(true);
  }


  // chunks of size > 4MB can not use checksums due to limits in the azure blob storage API.
  // chunks of size > 4MB can not use checksums due to limits in the azure blob storage API.
  @Test
  public void testNoChecksumRequiredForLargeChunk() {
    AzureAsyncReader reader = new AzureAsyncReader(AZURE_ENDPOINT,
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), "0", true, mock(AsyncHttpClient.class),
      true,
      mock(AsyncReadWithRetry.class));
    assertTrue(reader.requireChecksum(4194304));
    assertTrue(!reader.requireChecksum(4194305));
  }

  @Test public void testRespectNoChecksumArg() {
    AzureAsyncReader reader = new AzureAsyncReader(AZURE_ENDPOINT,
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), "0", true, mock(AsyncHttpClient.class),
      false,
      mock(AsyncReadWithRetry.class));
    assertTrue(!reader.requireChecksum(4194304));
    assertTrue(!reader.requireChecksum(4194305));
  }

  void verifyTestPathNotFound(boolean checkVersion) {
    final String responseBody = "{\"error\":{\"code\":\"PathNotFound\",\"message\":\"The specified path does not exist." +
      "\\nRequestId:5b544bd0-c01f-0048-03f0-16bacd000000\\nTime:2020-04-20T08:51:53.7856703Z\"}}";
    final int responseCode = 404;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode, checkVersion);
    ByteBuf buf = Unpooled.buffer(20);

    try {
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(FileNotFoundException.class, e.getCause().getClass());
      assertTrue(e.getMessage().contains("PathNotFound"));
    } finally {
      verify(azureAsyncReader, times(1)).read(eq(0L), eq(20L), any(ChecksumVerifyingCompletionHandler.class), eq(0));
    }
  }

  @Test
  public void testPathNotFound() {
    verifyTestPathNotFound(true);
    verifyTestPathNotFound(false);
  }

  void verifyTestServerErrorsAndRetries(boolean checkVersion) {
    final String responseBody = "{\"error\":{\"code\":\"InternalServerError\",\"message\":\"Something wrong happened at the server." +
      "\\nRequestId:5b544bd0-c01f-0048-03f0-16bacd000000\\nTime:2020-04-20T08:51:53.7856703Z\"}}";
    final int responseCode = 500;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode, checkVersion);

    int len = 20;
    ChecksumVerifyingCompletionHandler responseHandler =
      new ChecksumVerifyingCompletionHandler(Unpooled.buffer(len), len);
    try {
      azureAsyncReader.read(0, len, responseHandler, 0).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(RuntimeException.class, e.getCause().getClass());
      assertTrue(e.getMessage().contains("InternalServerError"));
    } finally {
      // Verify each attempt explicitly.
      int expectedRetries = 10;
      for (int retryAttempt = 0; retryAttempt < expectedRetries; retryAttempt++) {
        AsyncReadWithRetry asyncReadWithRetry = azureAsyncReader.getAsyncReaderWithRetry();
        verify(asyncReadWithRetry).read(azureAsyncReader.getAsyncHttpClient(),
                azureAsyncReader.getRequestBuilderFunction(0, len, azureAsyncReader.getMetricLogger()),
                azureAsyncReader.getMetricLogger(), azureAsyncReader.getPath(), azureAsyncReader.getThreadName(),
                responseHandler, retryAttempt, azureAsyncReader.getBackoff());
      }
    }
  }

  @Test
  public void testServerErrorsAndRetries() {
    verifyTestServerErrorsAndRetries(true);
    verifyTestServerErrorsAndRetries(false);
  }

  @Test
  public void testReadFullySecureCase() {
    testSuccessHttpMode(true, true);
    testSuccessHttpMode(true, false);
  }

  @Test
  public void testReadFullyNonSecureCase() {
    testSuccessHttpMode(false, true);
    testSuccessHttpMode(false, false);
  }

  private void testSuccessHttpMode(boolean isSecure, boolean checkVersion) {
    // Prepare response
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);
    LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);

    byte[] responseBytes = getRandomBytes(20);
    when(response.getHeader(ChecksumVerifyingCompletionHandler.CHECKSUM_RESPONSE_HEADER))
      .thenReturn(md5Checksum(responseBytes));
    HttpResponseBodyPart responsePart = mock(HttpResponseBodyPart.class);
    when(responsePart.getBodyByteBuffer()).thenReturn(ByteBuffer.wrap(responseBytes));

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      // Validate URL
      Request req = invocationOnMock.getArgument(0, Request.class);
      String expectedPrefix = isSecure ? "https" : "http";
      assertEquals("Invalid request url",
        expectedPrefix + "://account.dfs.core.windows.net/container/directory%2Ffile_00.parquet", req.getUrl());

      // Validate Headers
      assertEquals("Invalid blob range header", "bytes=0-19", req.getHeaders().get("Range"));
      LocalDateTime dateHeaderVal = LocalDateTime.parse(req.getHeaders().get("Date"), DATE_RFC1123_FORMATTER);
      long dateHeaderDiffInSecs = Math.abs(dateHeaderVal.until(LocalDateTime.now(ZoneId.of("GMT")), ChronoUnit.SECONDS));
      assertTrue("Date header not set correctly", dateHeaderDiffInSecs < 2);

      if (checkVersion) {
        LocalDateTime versionHeaderVal = LocalDateTime.parse(req.getHeaders().get("If-Unmodified-Since"), DATE_RFC1123_FORMATTER);
        assertEquals("Version header not set correctly", 0, versionHeaderVal.until(versionDate, ChronoUnit.SECONDS));
      }

      assertEquals("Authz header not set correctly", req.getHeaders().get("Authorization"), "Bearer testtoken");
      assertNotNull(req.getHeaders().get("x-ms-client-request-id"));

      // Fill in response
      AsyncCompletionHandler<Response> responseHandler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      assertEquals(responseHandler.getClass(), ChecksumVerifyingCompletionHandler.class);

      responseHandler.onBodyPartReceived(responsePart);
      responseHandler.onStatusReceived(status);
      responseHandler.onCompleted(response);
      return resFuture;
    });

    AzureAsyncReader azureAsyncReader;
    if (checkVersion) {
      azureAsyncReader = getReader(String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()), isSecure, client);
    } else {
      azureAsyncReader = getReader("0", isSecure, client);
    }

      ByteBuf buf = Unpooled.buffer(20);
      int len = 20;
      azureAsyncReader.readFully(0, buf, 0, len).join();
      assertEquals(new String(buf.array()), new String(responseBytes));
      verify(azureAsyncReader).read(eq(0L), eq(20L), any(ChecksumVerifyingCompletionHandler.class), eq(0));
  }

  AzureAsyncReader getReader(String version, boolean isSecure, AsyncHttpClient client) {
    AsyncReadWithRetry asyncReadWithRetry = spy(new AsyncReadWithRetry(throwable -> {
      if (throwable.getMessage().contains("ConditionNotMet")) {
        return AsyncReadWithRetry.Error.PRECONDITION_NOT_MET;
      } else if (throwable.getMessage().contains("PathNotFound")) {
        return AsyncReadWithRetry.Error.PATH_NOT_FOUND;
      } else {
        return AsyncReadWithRetry.Error.UNKNOWN;
      }
    }));
    return spy(new AzureAsyncReader(AZURE_ENDPOINT,
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), version, isSecure, client, true, asyncReadWithRetry
    ));
  }

  @Test
  public void testAsyncReaderWithRandomCharacterInPath() {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);
    AzureAsyncReader azureAsyncReader = new AzureAsyncReader(AZURE_ENDPOINT,
      "account", new Path("/testdir/$#%&New Folder to test abc 123/0_0_0.parquet"),
      getMockAuthTokenProvider(), String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()),
      false, client, true
    );
  }

  @Test
  public void testAsyncHttpClientClosedError() {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    when(client.isClosed()).thenReturn(true);
    LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);

    AzureAsyncReader azureAsyncReader = spy(new AzureAsyncReader(AZURE_ENDPOINT,
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()),
      false, client, true
    ));

    try {
      azureAsyncReader.readFully(0, Unpooled.buffer(1), 0, 1);
      fail("Operation shouldn't proceed if client is closed");
    } catch (RuntimeException e) {
      assertEquals("AsyncHttpClient is closed", e.getMessage());
    }
  }

  private AzureAsyncReader prepareAsyncReader(final String responseBody, final int responseCode, boolean checkVersion) {
    // Prepare response
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(responseCode);
    when(response.getResponseBody()).thenReturn(responseBody);

    CompletableFuture<Response> future = new CompletableFuture<>(); //CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      AsyncCompletionHandler<Response> responseHandler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      assertEquals(responseHandler.getClass(), ChecksumVerifyingCompletionHandler.class);
      responseHandler.onStatusReceived(status);
      try {
        responseHandler.onCompleted(response);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
      return resFuture;
    });

    AzureAsyncReader azureAsyncReader;
    if (checkVersion) {
      LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);
      azureAsyncReader = getReader(String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()), true, client);
    } else {
      azureAsyncReader = getReader("0", true, client);
    }
    MetricsLogger metricsLogger = mock(MetricsLogger.class);
    when(azureAsyncReader.getMetricLogger()).thenReturn(metricsLogger);
    Function<Void, Request> requestFunction = unused -> mock(Request.class);

    when(azureAsyncReader.getRequestBuilderFunction(0, 20, metricsLogger)).thenReturn(requestFunction);
    return azureAsyncReader;
  }

  private byte[] getRandomBytes(int size) {
    byte[] arr = new byte[size];
    random.nextBytes(arr);
    return arr;
  }

  private AzureAuthTokenProvider getMockAuthTokenProvider() {
    AzureAuthTokenProvider authTokenProvider = mock(AzureAuthTokenProvider.class);
    when(authTokenProvider.checkAndUpdateToken()).thenReturn(false);
    when(authTokenProvider.getAuthzHeaderValue(any(Request.class))).thenReturn("Bearer testtoken");
    return authTokenProvider;
  }

  private String md5Checksum(byte[] bytes) {
    return Base64.getEncoder().encodeToString(DigestUtils.md5(bytes));
  }
}
