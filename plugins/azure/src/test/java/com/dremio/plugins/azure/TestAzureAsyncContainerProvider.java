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
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.Retryer;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.plugins.util.ContainerNotFoundException;
import com.google.common.io.ByteStreams;

/**
 * Tests for AzureAsyncContainerProvider
 */
public class TestAzureAsyncContainerProvider {
  private static final String AZURE_ENDPOINT = "dfs.core.windows.net";

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testListContainers() throws IOException, ExecutionException, InterruptedException {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    byte[] dfsCoreResponseBodyBytesPage1 = readStaticResponse("dfs-core-container-response-page1.json");
    RandomBytesResponseDispatcher dfsCoreResponseDispatcherPage1 = new RandomBytesResponseDispatcher(dfsCoreResponseBodyBytesPage1);

    byte[] dfsCoreResponseBodyBytesPage2 = readStaticResponse("dfs-core-container-response-page2.json");
    RandomBytesResponseDispatcher dfsCoreResponseDispatcherPage2 = new RandomBytesResponseDispatcher(dfsCoreResponseBodyBytesPage2);

    byte[] dfsCoreEmptyResponseBytes = readStaticResponse("dfs-core-container-empty.json");
    RandomBytesResponseDispatcher dfsCoreEmptyResponseDispatcher = new RandomBytesResponseDispatcher(dfsCoreEmptyResponseBytes);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);
    when(resFuture.get()).thenReturn(response);

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      Request req = invocationOnMock.getArgument(0, Request.class);
      AsyncCompletionHandler handler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);

      assertTrue(req.getUrl().startsWith("https://azurestoragev2hier.dfs.core.windows.net"));
      assertNotNull(req.getHeaders().get("Date"));
      assertNotNull(req.getHeaders().get("x-ms-client-request-id"));
      assertEquals("2019-07-07", req.getHeaders().get("x-ms-version")); // edit only if you're upgrading client
      assertEquals("Bearer testtoken", req.getHeaders().get("Authorization"));
      List<NameValuePair> queryParams = URLEncodedUtils.parse(new URI(req.getUrl()), StandardCharsets.UTF_8);
      String continuationKey = queryParams.stream()
        .filter(param -> param.getName().equalsIgnoreCase("continuation")).findAny()
        .map(NameValuePair::getValue).orElse("");

      // Return empty response if continuation key is not present. Return data in continuation call.
      // This is to ensure that the plugin makes another call when there's no data but continuation key present.
      if ("page1container1".equals(continuationKey)) {
        when(response.getHeader("x-ms-continuation")).thenReturn("page2container1");
        while (dfsCoreResponseDispatcherPage1.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreResponseDispatcherPage1.getNextBodyPart());
        }
      } else if ("page2container1".equals(continuationKey)) {
        when(response.getHeader("x-ms-continuation")).thenReturn("");
        while (dfsCoreResponseDispatcherPage2.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreResponseDispatcherPage2.getNextBodyPart());
        }
      } else {
        when(response.getHeader("x-ms-continuation")).thenReturn("page1container1");
        while (dfsCoreEmptyResponseDispatcher.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreEmptyResponseDispatcher.getNextBodyPart());
        }
      }

      handler.onStatusReceived(status);
      handler.onCompleted(response);
      return resFuture;
    });

    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);

    List<String> receivedContainers = containerProvider.getContainerCreators()
      .map(AzureStorageFileSystem.ContainerCreatorImpl.class::cast)
      .map(AzureStorageFileSystem.ContainerCreatorImpl::getName)
      .collect(Collectors.toList());

    List<String> expectedContainers = Arrays.asList("page1container1", "page1container2", "page1container3", "page2container1", "page2container2", "page2container3");
    assertEquals(expectedContainers, receivedContainers);
  }

  @Test
  public void testListContainersWithRetry() throws IOException, ExecutionException, InterruptedException {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);
    HttpResponseStatus failedStatus = mock(HttpResponseStatus.class);
    when(failedStatus.getStatusCode()).thenReturn(500);

    byte[] dfsCoreResponseBodyBytes = readStaticResponse("dfs-core-container-response-page1.json");
    RandomBytesResponseDispatcher dfsCoreResponseDispatcher = new RandomBytesResponseDispatcher(dfsCoreResponseBodyBytes);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);
    when(resFuture.get()).thenReturn(response);

    AtomicInteger retryAttemptNo = new AtomicInteger(0);
    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      AsyncCompletionHandler handler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);
      if (retryAttemptNo.incrementAndGet() < 10) {
        handler.onStatusReceived(failedStatus);
      } else {
        while (dfsCoreResponseDispatcher.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreResponseDispatcher.getNextBodyPart());
        }
        handler.onStatusReceived(status);
      }
      handler.onCompleted(response);
      return resFuture;
    });

    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);

    List<String> receivedContainers = containerProvider.getContainerCreators()
      .map(AzureStorageFileSystem.ContainerCreatorImpl.class::cast)
      .map(AzureStorageFileSystem.ContainerCreatorImpl::getName)
      .collect(Collectors.toList());

    List<String> expectedContainers = Arrays.asList("page1container1", "page1container2", "page1container3");
    assertEquals(expectedContainers, receivedContainers);
  }

  @Test
  public void testDoesContainerExists() throws ExecutionException, InterruptedException {
    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    when(response.getStatusCode()).thenReturn(200);
    ListenableFuture<Response> future = mock(ListenableFuture.class);
    when(future.get()).thenReturn(response);
    when(client.executeRequest(any(Request.class))).thenReturn(future);

    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);
    containerProvider.assertContainerExists("container");
  }

  @Test
  public void testWhiteListValidation() throws IOException, ExecutionException, InterruptedException {
    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    when(response.getStatusCode()).thenReturn(404);
    ListenableFuture<Response> future = mock(ListenableFuture.class);
    when(future.get()).thenReturn(response);
    when(client.executeRequest(any(Request.class))).thenReturn(future);

    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true, new String[] {"tempContainer"}, null);

    thrown.expect(UserException.class);
    thrown.expectMessage("Failure while validating existence of container tempContainer. Error: rootPath null  in container tempContainer is not found - [404 null]");
    containerProvider.verfiyContainersExist();
  }

  @Test
  public void testDoesContainerExistsNotFound() throws ExecutionException, InterruptedException {
    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    when(response.getStatusCode()).thenReturn(404);
    when(response.getStatusText()).thenReturn("The specified filesystem does not exist.");
    ListenableFuture<Response> future = mock(ListenableFuture.class);
    when(future.get()).thenReturn(response);
    when(client.executeRequest(any(Request.class))).thenReturn(future);

    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
            client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);
    try {
      containerProvider.assertContainerExists("container");
      fail("Expecting exception");
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      assertEquals(ContainerNotFoundException.class, e.getCause().getClass());
      assertEquals("rootPath null  in container container is not found - [404 The specified filesystem does not exist.]", e.getCause().getMessage());
    }

    // Ensure no retries attempted
    verify(client, times(1)).executeRequest(any(Request.class));
  }

  @Test
  public void testDoesContainerExistsAccessDenied() throws ExecutionException, InterruptedException {
    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    when(response.getStatusCode()).thenReturn(403);
    ListenableFuture<Response> future = mock(ListenableFuture.class);
    when(future.get()).thenReturn(response);
    when(client.executeRequest(any(Request.class))).thenReturn(future);

    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
      client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);
    try {
      containerProvider.assertContainerExists("container");
      fail("Expecting exception");
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      assertTrue(e.getCause() instanceof ContainerAccessDeniedException);
      assertEquals("Either access to container container was denied or Container container does not exist - [403 null]", e.getCause().getMessage());
    }
    // Ensure no retries attempted
    verify(client, times(1)).executeRequest(any(Request.class));
  }

  @Test
  public void testListContainersExtraAttrs() throws IOException, ExecutionException, InterruptedException {
    AsyncHttpClient client = mock(AsyncHttpClient.class);
    Response response = mock(Response.class);
    when(response.getHeader(any(String.class))).thenReturn("");
    HttpResponseStatus status = mock(HttpResponseStatus.class);
    when(status.getStatusCode()).thenReturn(206);

    byte[] dfsCoreResponseBodyBytesPage1 = readStaticResponse("dfs-core-container-response-extra-attrs.json");
    RandomBytesResponseDispatcher dfsCoreResponseDispatcherPage1 = new RandomBytesResponseDispatcher(dfsCoreResponseBodyBytesPage1);

    byte[] dfsCoreResponseBodyBytesPage2 = readStaticResponse("dfs-core-container-response-page2.json");
    RandomBytesResponseDispatcher dfsCoreResponseDispatcherPage2 = new RandomBytesResponseDispatcher(dfsCoreResponseBodyBytesPage2);

    byte[] dfsCoreEmptyResponseBytes = readStaticResponse("dfs-core-container-empty.json");
    RandomBytesResponseDispatcher dfsCoreEmptyResponseDispatcher = new RandomBytesResponseDispatcher(dfsCoreEmptyResponseBytes);

    CompletableFuture<Response> future = CompletableFuture.completedFuture(response);
    ListenableFuture<Response> resFuture = mock(ListenableFuture.class);
    when(resFuture.toCompletableFuture()).thenReturn(future);
    when(resFuture.get()).thenReturn(response);

    when(client.executeRequest(any(Request.class), any(AsyncCompletionHandler.class))).then(invocationOnMock -> {
      Request req = invocationOnMock.getArgument(0, Request.class);
      AsyncCompletionHandler handler = invocationOnMock.getArgument(1, AsyncCompletionHandler.class);

      assertTrue(req.getUrl().startsWith("https://azurestoragev2hier.dfs.core.windows.net"));
      assertNotNull(req.getHeaders().get("Date"));
      assertNotNull(req.getHeaders().get("x-ms-client-request-id"));
      assertEquals("2019-07-07", req.getHeaders().get("x-ms-version")); // edit only if you're upgrading client
      assertEquals("Bearer testtoken", req.getHeaders().get("Authorization"));
      List<NameValuePair> queryParams = URLEncodedUtils.parse(new URI(req.getUrl()), StandardCharsets.UTF_8);
      String continuationKey = queryParams.stream()
              .filter(param -> param.getName().equalsIgnoreCase("continuation")).findAny()
              .map(NameValuePair::getValue).orElse("");

      // Return empty response if continuation key is not present. Return data in continuation call.
      // This is to ensure that the plugin makes another call when there's no data but continuation key present.
      if ("page1container1".equals(continuationKey)) {
        when(response.getHeader("x-ms-continuation")).thenReturn("page2container1");
        while (dfsCoreResponseDispatcherPage1.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreResponseDispatcherPage1.getNextBodyPart());
        }
      } else if ("page2container1".equals(continuationKey)) {
        when(response.getHeader("x-ms-continuation")).thenReturn("");
        while (dfsCoreResponseDispatcherPage2.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreResponseDispatcherPage2.getNextBodyPart());
        }
      } else {
        when(response.getHeader("x-ms-continuation")).thenReturn("page1container1");
        while (dfsCoreEmptyResponseDispatcher.isNotFinished()) {
          handler.onBodyPartReceived(dfsCoreEmptyResponseDispatcher.getNextBodyPart());
        }
      }

      handler.onStatusReceived(status);
      handler.onCompleted(response);
      return resFuture;
    });

    AzureStorageFileSystem parentClass = mock(AzureStorageFileSystem.class);
    AzureAuthTokenProvider authTokenProvider = getMockAuthTokenProvider();
    AzureAsyncContainerProvider containerProvider = new AzureAsyncContainerProvider(
            client, AZURE_ENDPOINT, "azurestoragev2hier", authTokenProvider, parentClass, true);

    List<String> receivedContainers = containerProvider.getContainerCreators()
            .map(AzureStorageFileSystem.ContainerCreatorImpl.class::cast)
            .map(AzureStorageFileSystem.ContainerCreatorImpl::getName)
            .collect(Collectors.toList());

    List<String> expectedContainers = Arrays.asList("page1container1", "page1container2", "page1container3", "page2container1", "page2container2", "page2container3");
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
