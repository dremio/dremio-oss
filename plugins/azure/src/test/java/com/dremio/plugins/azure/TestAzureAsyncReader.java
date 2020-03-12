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
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.fs.Path;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 * Tests for AzureAsyncReader
 */
public class TestAzureAsyncReader {

  private static final DateTimeFormatter DATE_RFC1123_FORMATTER = DateTimeFormatter
    .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
    .withZone(ZoneId.of("UTC"))
    .withLocale(Locale.US);

  private final Random random = new Random();

  @Test
  public void testFileVersionChanged() {
    final String responseBody =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Error>" +
        "   <Code>ConditionNotMet</Code>" +
        "   <Message>" +
        "       The condition specified using HTTP conditional header(s) is not met. " +
        "       RequestId:60e08b37-801e-0084-046d-f3b2cc000000 " +
        "       Time:2019-04-15T09:26:02.3121974Z" +
        "   </Message>" +
        "</Error>";
    final int responseCode = 412;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode);
    ByteBuf buf = Unpooled.buffer(20);

    try {
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(FileNotFoundException.class, e.getCause().getClass());
      assertTrue(e.getCause().getMessage().contains("Version of file has changed"));
    } finally {
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 0);
    }
  }

  @Test
  public void testBlobNotFound() {
    final String responseBody =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Error>" +
        "   <Code>BlobNotFound</Code>" +
        "   <Message>" +
        "      The specified blob does not exist." +
        "      RequestId:44ac97c2-601e-00e4-60a6-bf9b5a000000" +
        "      Time:2019-12-31T06:49:04.7710349Z" +
        "   </Message>" +
        "</Error>";
    final int responseCode = 404;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode);
    ByteBuf buf = Unpooled.buffer(20);

    try {
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(FileNotFoundException.class, e.getCause().getClass());
      assertTrue(e.getMessage().contains("BlobNotFound"));
    } finally {
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 0);
    }
  }

  @Test
  public void testServerErrorsAndRetries() {
    final String responseBody =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Error>" +
        "   <Code>InternalServerError</Code>" +
        "   <Message>" +
        "      Something wrong happened at the server." +
        "      RequestId:44ac97c2-601e-00e4-60a6-bf9b5a000000" +
        "      Time:2019-12-31T06:49:04.7710349Z" +
        "   </Message>" +
        "</Error>";
    final int responseCode = 500;
    AzureAsyncReader azureAsyncReader = prepareAsyncReader(responseBody, responseCode);
    ByteBuf buf = Unpooled.buffer(20);

    try {
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      fail("Should fail because of failing condition match");
    } catch (Exception e) {
      assertEquals(RuntimeException.class, e.getCause().getClass());
      assertTrue(e.getMessage().contains("InternalServerError"));
    } finally {
      // Retries happened 4 times. Verify each attempt explicitly.
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 0);
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 1);
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 2);
      verify(azureAsyncReader, times(1)).read(0, buf, 0, 20, 3);
    }
  }

  @Test
  public void testReadFullySecureCase() {
    testSuccessHttpMode(true);
  }

  @Test
  public void testReadFullyNonSecureCase() {
    testSuccessHttpMode(false);
  }

  private void testSuccessHttpMode(boolean isSecure) {
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
    HttpResponseBodyPart responsePart = mock(HttpResponseBodyPart.class);
    when(responsePart.getBodyByteBuffer()).thenReturn(ByteBuffer.wrap(responseBytes));

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      // Validate URL
      Request req = invocationOnMock.getArgument(0, Request.class);
      String expectedPrefix = isSecure ? "https" : "http";
      assertEquals("Invalid request url",
        expectedPrefix + "://account.blob.core.windows.net/container/directory%2Ffile_00.parquet", req.getUrl());

      // Validate Headers
      assertEquals("Invaid blob range header", "bytes=0-19", req.getHeaders().get("x-ms-range"));
      LocalDateTime dateHeaderVal = LocalDateTime.parse(req.getHeaders().get("Date"), DATE_RFC1123_FORMATTER);
      long dateHeaderDiffInSecs = Math.abs(dateHeaderVal.until(LocalDateTime.now(ZoneId.of("GMT")), ChronoUnit.SECONDS));
      assertTrue("Date header not set correctly", dateHeaderDiffInSecs < 2);

      LocalDateTime versionHeaderVal = LocalDateTime.parse(req.getHeaders().get("If-Unmodified-Since"), DATE_RFC1123_FORMATTER);
      assertEquals("Version header not set correctly", 0, versionHeaderVal.until(versionDate, ChronoUnit.SECONDS));

      assertEquals("Authz header not set correctly", req.getHeaders().get("Authorization"), "Bearer testtoken");
      assertNotNull(req.getHeaders().get("x-ms-client-request-id"));

      // Fill in response
      AsyncCompletionHandler<Response> responseHandler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      assertEquals(responseHandler.getClass(), BufferBasedCompletionHandler.class);

      responseHandler.onBodyPartReceived(responsePart);
      responseHandler.onStatusReceived(status);
      responseHandler.onCompleted(response);
      return resFuture;
    });

    AzureAsyncReader azureAsyncReader = spy(new AzureAsyncReader(
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()),
      isSecure, client
    ));

    try {
      ByteBuf buf = Unpooled.buffer(20);
      azureAsyncReader.readFully(0, buf, 0, 20).get();
      assertEquals(new String(buf.array()), new String(responseBytes));
      verify(azureAsyncReader).read(0, buf, 0, 20, 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAsyncHttpClientClosedError() {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    when(client.isClosed()).thenReturn(true);
    LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);

    AzureAsyncReader azureAsyncReader = spy(new AzureAsyncReader(
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()),
      false, client
    ));

    try {
      azureAsyncReader.readFully(0, Unpooled.buffer(1), 0, 1);
      fail("Operation shouldn't proceed if client is closed");
    } catch (RuntimeException e) {
      assertEquals("AsyncHttpClient is closed", e.getMessage());
    }
  }


  private AzureAsyncReader prepareAsyncReader(final String responseBody, final int responseCode) {
    // Prepare response
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(responseCode);
    when(response.getResponseBody()).thenReturn(responseBody);

    CompletableFuture<Response> future = new CompletableFuture<>(); //CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);
    LocalDateTime versionDate = LocalDateTime.now(ZoneId.of("GMT")).minusDays(2);

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      AsyncCompletionHandler<Response> responseHandler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      assertEquals(responseHandler.getClass(), BufferBasedCompletionHandler.class);
      responseHandler.onStatusReceived(status);
      try {
        responseHandler.onCompleted(response);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
      return resFuture;
    });

    AzureAsyncReader azureAsyncReader = spy(new AzureAsyncReader(
      "account", new Path("container/directory/file_00.parquet"),
      getMockAuthTokenProvider(), String.valueOf(versionDate.atZone(ZoneId.of("GMT")).toInstant().toEpochMilli()),
      true, client
    ));
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
}
