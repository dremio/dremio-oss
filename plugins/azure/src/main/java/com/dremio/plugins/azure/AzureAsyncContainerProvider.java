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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.http.client.utils.URIBuilder;
import org.apache.poi.util.IOUtils;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.uri.Uri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.dremio.common.util.Retryer;
import com.dremio.plugins.azure.utils.AzureAsyncHttpClientUtils;
import com.dremio.plugins.util.ContainerFileSystem;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * Container provider based on AsyncHttpClient
 */
public class AzureAsyncContainerProvider implements ContainerProvider {

  private final AzureAuthTokenProvider authProvider;
  private final AzureStorageFileSystem parent;
  private final String account;
  private final boolean isSecure;
  private final AsyncHttpClient asyncHttpClient;

  public AzureAsyncContainerProvider(final AsyncHttpClient asyncHttpClient,
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
    return StreamSupport.stream(
      Spliterators.spliteratorUnknownSize(
        new ContainerIterator(asyncHttpClient, account, authProvider, isSecure), Spliterator.ORDERED), false)
      .map(c -> new AzureStorageFileSystem.ContainerCreatorImpl(parent, c));
  }

}

class ContainerIterator extends AbstractIterator<String> {
  private static final int PAGE_SIZE = 100; // Approx no of rows shown on dremio console
  private static final String EXPRESSION = "/EnumerationResults/Containers/Container";
  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncContainerProvider.class);
  private static final int BASE_MILLIS_TO_WAIT = 250; // set to the average latency of an async read
  private static final int MAX_MILLIS_TO_WAIT = 10 * BASE_MILLIS_TO_WAIT;

  private final String uri;
  private final AsyncHttpClient asyncHttpClient;
  private final AzureAuthTokenProvider authProvider;
  private final Retryer retryer; // This class is already inheriting different class, we cannot add ExponentialBackoff

  private String marker = null;
  private boolean hasMorePages = true;
  private Iterator<String> iterator = null;
  private List<String> containerNames = Lists.newArrayList();

  ContainerIterator(final AsyncHttpClient asyncHttpClient,
                    final String account,
                    final AzureAuthTokenProvider authProvider,
                    final boolean isSecure) {
    this.authProvider = authProvider;
    this.asyncHttpClient = asyncHttpClient;
    this.uri = AzureAsyncHttpClientUtils.getBaseEndpointURL(account, isSecure);
    retryer = new Retryer.Builder()
      .retryIfExceptionOfType(RuntimeException.class)
      .setWaitStrategy(Retryer.WaitStrategy.EXPONENTIAL, BASE_MILLIS_TO_WAIT, MAX_MILLIS_TO_WAIT)
      .setMaxRetries(4).build();
  }

  // https://docs.microsoft.com/en-us/rest/api/storageservices/list-containers2
  private Request buildRequest() throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(uri);
    uriBuilder.addParameter("comp", "list");
    uriBuilder.addParameter("maxresults", String.valueOf(PAGE_SIZE));
    if (marker != null) {
      uriBuilder.addParameter("marker", marker);
    }

    RequestBuilder requestBuilder = AzureAsyncHttpClientUtils.newDefaultRequestBuilder()
      .setUri(Uri.create(uriBuilder.build().toASCIIString()));
    return requestBuilder.build();
  }

  void readNextPage() throws Retryer.OperationFailedAfterRetriesException {
    retryer.call(() -> {
      ByteArrayInputStream inputStream = null;
      try(ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        Request request = buildRequest();
        request.getHeaders().add("Authorization", authProvider.getAuthzHeaderValue(request));
        asyncHttpClient.executeRequest(request, new ListContainersResponseProcessor(baos)).get();
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder builder = factory.newDocumentBuilder();
        inputStream = new ByteArrayInputStream(baos.toByteArray());
        final Document doc = builder.parse(inputStream);
        final XPath xPath = XPathFactory.newInstance().newXPath();
        NodeList nodeList = (NodeList) xPath.compile(EXPRESSION).evaluate(
          doc, XPathConstants.NODESET);
        containerNames.clear();
        for (int i = 0; i < nodeList.getLength(); i++) {
          Node node = nodeList.item(i);
          containerNames.add(node.getFirstChild().getFirstChild().getNodeValue());
        }
        iterator = containerNames.iterator();
        nodeList = (NodeList) xPath.compile("/EnumerationResults/NextMarker").evaluate(doc, XPathConstants.NODESET);
        if (nodeList.item(0).hasChildNodes()) {
          marker = nodeList.item(0).getFirstChild().getNodeValue();
        } else {
          hasMorePages = false;
          marker = null;
        }
        return true;
      } catch (Exception e) {
        // Throw ExecutionException for non-retryable cases.
        if (e.getMessage().contains("UnknownHostException")) {
          // Do not retry
          logger.error("Error while reading containers from " + uri, e);
          throw new ExecutionException(e);
        }

        // retryable
        throw new RuntimeException(e);
      } finally {
        IOUtils.closeQuietly(inputStream);
      }
    });
  }

  @Override
  protected String computeNext() {
    try {
      boolean resultsLeftInPage = iterator != null && iterator.hasNext();
      if (!resultsLeftInPage && hasMorePages) {
        readNextPage();
        resultsLeftInPage = iterator.hasNext();
      }
      return resultsLeftInPage ? iterator.next() : endOfData();
    } catch (Retryer.OperationFailedAfterRetriesException e) {
      // Failed after retries
      logger.error("Error while reading azure storage containers.", e);
      throw new AzureStoragePluginException(e);
    }
  }

  class ListContainersResponseProcessor extends AsyncCompletionHandlerBase {
    private boolean requestFailed = false;
    private final ByteArrayOutputStream baos;

    ListContainersResponseProcessor(final ByteArrayOutputStream baos) {
      this.baos = baos;
    }

    @Override
    public State onStatusReceived(final HttpResponseStatus status) throws Exception {
      requestFailed = (status.getStatusCode() >= 400);
      return super.onStatusReceived(status);
    }

    @Override
    public Response onCompleted(final Response response) {
      if (requestFailed) {
        logger.error("Error while listing Azure containers. Server response is {}.", response.getResponseBody());
        throw new RuntimeException(response.getResponseBody());
      }
      return response;
    }

    @Override
    public State onBodyPartReceived(final HttpResponseBodyPart content) throws Exception {
      if (requestFailed) {
        return super.onBodyPartReceived(content);
      }
      baos.write(content.getBodyPartBytes());
      return State.CONTINUE;
    }
  }
}
