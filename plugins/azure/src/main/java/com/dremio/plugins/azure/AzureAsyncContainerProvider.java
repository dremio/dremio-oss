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

import static com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils.toHttpDateFormat;

import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.util.Retryer;
import com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils;
import com.dremio.plugins.util.ContainerFileSystem;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.google.common.collect.AbstractIterator;


/**
 * Container provider based on AsyncHttpClient.
 * Consumers of the ContainerCreator stream at {@link #getContainerCreators()} should ensure that this stream isn't
 * shared across threads and the operation is called in a thread safe manner.
 */
@NotThreadSafe
public class AzureAsyncContainerProvider implements ContainerProvider {
  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncContainerProvider.class);

  private final AzureAuthTokenProvider authProvider;
  private final AzureStorageFileSystem parent;
  private final String account;
  private final boolean isSecure;
  private final AsyncHttpClient asyncHttpClient;

  AzureAsyncContainerProvider(final AsyncHttpClient asyncHttpClient,
                                     final String account,
                                     final AzureAuthTokenProvider authProvider,
                                     final AzureStorageFileSystem parent,
                                     boolean isSecure) {
    this.authProvider = authProvider;
    this.parent = parent;
    this.account = account;
    this.isSecure = isSecure;
    this.asyncHttpClient = asyncHttpClient;
  }

  @Override
  public Stream<ContainerFileSystem.ContainerCreator> getContainerCreators() {
    Iterator<String> containerIterator = new DFSContainerIterator(asyncHttpClient, account, authProvider, isSecure);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(containerIterator, Spliterator.ORDERED), false)
      .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c));
  }

  static class DFSContainerIterator extends AbstractIterator<String> {
    private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
    private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;
    private static final int EMPTY_CONTINUATION_RETRIES = 10;
    private static final int PAGE_SIZE = 100;  // Approx no of rows shown on dremio console
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String uri;
    private final AsyncHttpClient asyncHttpClient;
    private final AzureAuthTokenProvider authProvider;
    private final Retryer retryer; // This class is already inheriting different class, we cannot add ExponentialBackoff

    private String continuation = "";
    private boolean hasMorePages = true;
    private Iterator<String> iterator = Collections.emptyIterator();

    DFSContainerIterator(final AsyncHttpClient asyncHttpClient,
                         final String account,
                         final AzureAuthTokenProvider authProvider,
                         final boolean isSecure) {
      this.authProvider = authProvider;
      this.asyncHttpClient = asyncHttpClient;
      this.uri = AzureAsyncHttpClientUtils.getBaseEndpointURL(account, isSecure);
      retryer = new Retryer.Builder()
        .retryIfExceptionOfType(RuntimeException.class)
        .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, BASE_MILLIS_TO_WAIT, MAX_MILLIS_TO_WAIT)
        .setMaxRetries(10).build();
    }

    private Request buildRequest() throws URISyntaxException {
      // API - https://docs.microsoft.com/en-gb/rest/api/storageservices/datalakestoragegen2/path/list
      URIBuilder uriBuilder = new URIBuilder(uri);
      uriBuilder.addParameter("resource", "account");
      uriBuilder.addParameter("continuation", continuation);
      uriBuilder.addParameter("maxResults", String.valueOf(PAGE_SIZE));

      RequestBuilder requestBuilder = AzureAsyncHttpClientUtils.newDefaultRequestBuilder()
        .addHeader("x-ms-date", toHttpDateFormat(System.currentTimeMillis()))
        .setUri(Uri.create(uriBuilder.build().toASCIIString()));
      return requestBuilder.build();
    }

    void readNextPage() throws Retryer.OperationFailedAfterRetriesException {
      retryer.call(() -> {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          final Request request = buildRequest();
          request.getHeaders().add("Authorization", authProvider.getAuthzHeaderValue(request));
          final Response response = asyncHttpClient.executeRequest(request, new BAOSBasedCompletionHandler(baos)).get();
          continuation = response.getHeader("x-ms-continuation");
          if (StringUtils.isEmpty(continuation)) {
            hasMorePages = false;
          }
          FileSystemListStub responseStubs = OBJECT_MAPPER.readValue(baos.toByteArray(), FileSystemListStub.class);
          iterator = responseStubs.filesystems.stream().map(FileSystemStub::getName).collect(Collectors.toList()).iterator();
          return true;
        } catch (MismatchedInputException e) {
          logger.warn("Empty response while reading azure containers " + e.getMessage());
          iterator = Collections.emptyIterator();
          return true;
        } catch (Exception e) {
          // Throw ExecutionException for non-retryable cases.
          if (StringUtils.isNotEmpty(e.getMessage()) && e.getMessage().contains("UnknownHostException")) {
            // Do not retry
            logger.error("Error while reading containers from " + uri, e);
            throw new ExecutionException(e);
          }

          // retryable
          throw new RuntimeException(e);
        }
      });
    }


    @Override
    protected String computeNext() {
      try {
        // Continuation key could be a non null value even when there are no results. Hence, we repeat the calls until we have the continuation.
        int continuationRetryLocalCnt = EMPTY_CONTINUATION_RETRIES;
        while (!iterator.hasNext() && hasMorePages && continuationRetryLocalCnt-- > 0) {
          readNextPage();
        }

        return iterator.hasNext() ? iterator.next() : endOfData();
      } catch (Retryer.OperationFailedAfterRetriesException e) {
        // Failed after retries
        logger.error("Error while reading azure storage containers.", e);
        throw new AzureStoragePluginException(e);
      }
    }

    @JsonAutoDetect
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class FileSystemListStub {
      @JsonProperty("filesystems")
      private List<FileSystemStub> filesystems = new ArrayList<>();
    }

    @JsonIgnoreProperties(value = { "etag", "lastModified" })
    @JsonAutoDetect
    public static class FileSystemStub {
      @JsonProperty("name")
      private String name;

      public String getName() {
        return name;
      }
    }
  }
}
