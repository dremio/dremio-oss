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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Test;

import com.google.common.io.ByteStreams;

/**
 * Tests for AzureAsyncContainerProvider
 */
public class TestAzureAsyncContainerProvider {

  @Test
  public void testListContainersChunkedBytesMultiPagedResult() throws IOException {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    byte[] page1ResponseBodyBytes = readStaticResponse("container-response-page1.xml");
    RandomBytesResponseDispatcher page1ResponseDispatcher = new RandomBytesResponseDispatcher(page1ResponseBodyBytes);

    byte[] page2ResponseBodyBytes = readStaticResponse("container-response-page2.xml");
    RandomBytesResponseDispatcher page2ResponseDispatcher = new RandomBytesResponseDispatcher(page2ResponseBodyBytes);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      Request req = invocationOnMock.getArgument(0, Request.class);
      assertTrue(req.getUrl().startsWith("https://azurestoragev2hier.blob.core.windows.net?comp=list&maxresults=100"));
      assertNotNull(req.getHeaders().get("Date"));
      assertNotNull(req.getHeaders().get("x-ms-client-request-id"));
      assertEquals("2019-02-02", req.getHeaders().get("x-ms-version")); // edit only if you're upgrading client
      assertEquals("Bearer testtoken", req.getHeaders().get("Authorization"));

      AsyncCompletionHandler handler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      // First page result has following as NextMarker. Next request is made with same criteria.
      //HttpResponseBodyPart responsePart = req.getUrl().endsWith("marker=%2Fazurestoragev2hier%2Ftestfs2") ? page2ResponsePart : page1ResponsePart;

      boolean isLastPage = req.getUrl().endsWith("marker=%2Fazurestoragev2hier%2Ftestfs2");

      // Dispatch responses in chunks
      if (isLastPage) {
        while (page2ResponseDispatcher.isNotFinished()) {
          handler.onBodyPartReceived(page2ResponseDispatcher.getNextBodyPart());
        }
      } else {
        while(page1ResponseDispatcher.isNotFinished()) {
          handler.onBodyPartReceived(page1ResponseDispatcher.getNextBodyPart());
        }
      }

      handler.onStatusReceived(status);
      handler.onCompleted(response);
      return resFuture;
    });

    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, "azurestoragev2hier", authTokenProvider, parentClass, true);

    List<String> receivedContainers = containerProvider.getContainerCreators()
      .map(AzureStorageFileSystem.ContainerCreatorImpl.class::cast)
      .map(AzureStorageFileSystem.ContainerCreatorImpl::getName)
      .collect(Collectors.toList());

    List<String> expectedContainers = Arrays.asList("bar", "foo", "helm-test-dist-ryan", "mraodist", "testdata", "testfs", "testfs2");
    assertEquals(expectedContainers, receivedContainers);
  }


  @Test
  public void testListContainersWithRetry() throws IOException {
    // Fail in first n-1 permitted attempts and succeed only in the final one.

    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);

    HttpResponseStatus failedStatus = mock(HttpResponseStatus.class);
    when(failedStatus.getStatusCode()).thenReturn(500);

    HttpResponseStatus successStatus = mock(HttpResponseStatus.class);
    when(successStatus.getStatusCode()).thenReturn(200);

    HttpResponseBodyPart page2ResponsePart = mock(HttpResponseBodyPart.class);
    byte[] page2ResponseBodyBytes = readStaticResponse("container-response-page2.xml");
    when(page2ResponsePart.getBodyPartBytes()).thenReturn(page2ResponseBodyBytes);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);

    AtomicInteger retryAttemptNo = new AtomicInteger(0);
    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      AsyncCompletionHandler handler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      // First page result has following as NextMarker. Next request is made with same criteria.
      if (retryAttemptNo.incrementAndGet() < 4) {
        handler.onStatusReceived(failedStatus);
      } else {
        handler.onStatusReceived(successStatus);
        handler.onBodyPartReceived(page2ResponsePart);
      }
      handler.onCompleted(response);
      return resFuture;
    });

    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, "azurestoragev2hier", authTokenProvider, parentClass, true);

    List<String> receivedContainers = containerProvider.getContainerCreators()
      .map(AzureStorageFileSystem.ContainerCreatorImpl.class::cast)
      .map(AzureStorageFileSystem.ContainerCreatorImpl::getName)
      .collect(Collectors.toList());

    List<String> expectedContainers = Arrays.asList("testfs2");
    assertEquals(expectedContainers, receivedContainers);
  }


  private byte[] readStaticResponse(String fileName) throws IOException {
    // response is big, hence kept in a separate file within test/resources folder
    try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(fileName)) {
      return ByteStreams.toByteArray(is);
    }
  }

  private AzureAuthTokenProvider getMockAuthTokenProvider() {
    AzureAuthTokenProvider authTokenProvider = mock(AzureAuthTokenProvider.class);
    when(authTokenProvider.checkAndUpdateToken()).thenReturn(false);
    when(authTokenProvider.getAuthzHeaderValue(any(Request.class))).thenReturn("Bearer testtoken");
    return authTokenProvider;
  }
}
