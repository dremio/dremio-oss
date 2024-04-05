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

import com.google.common.base.Preconditions;
import io.netty.handler.codec.http.HttpHeaderNames;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.client.utils.URIBuilder;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;
import org.slf4j.Logger;

/**
 * Wrapper around an {@link ADLStoreClient} client. Provides public methods to invoke
 * package-private methods of the underlying {@link ADLStoreClient} client.
 */
public class ADLSClient {

  public ADLStoreClient getClient() {
    return client;
  }

  private final ADLStoreClient client;

  public ADLSClient(ADLStoreClient client) {
    this.client = client;
  }

  /**
   * Calls the underlying client's {@code getExceptionFromResponse} to create an {@link
   * ADLException} from a {@link OperationResponse}
   *
   * @param resp
   * @param path
   * @return
   * @throws Exception
   */
  public IOException getExceptionFromResponse(OperationResponse resp, String path)
      throws Exception {
    throw client.getExceptionFromResponse(resp, String.format("Error reading file %s", path));
  }

  /**
   * Gets the access token associated with the underlying {@link ADLStoreClient} client.
   *
   * @throws IOException
   */
  public String getAccessToken() throws IOException {
    return client.getAccessToken();
  }

  /** Gets the ADLS account name associated with the underlying client */
  public String getAccountName() {
    return client.getAccountName();
  }

  /**
   * Public method to access the package private {@link Operation.OPEN} enum
   *
   * @return
   */
  public static Operation getOpenOperation() {
    return Operation.OPEN;
  }

  /**
   * Sets the {@link ADLStoreOptions} options for the underlying client.
   *
   * @param option
   * @throws IOException
   */
  public void setOptions(ADLStoreOptions option) throws IOException {
    client.setOptions(option);
  }

  /**
   * Gets the metadata/information about the given file by calling the underlying client's {@code
   * getDirectory} method.
   *
   * @throws IOException
   */
  public DirectoryEntry getDirectoryEntry(String testFile) throws IOException {
    return client.getDirectoryEntry(testFile);
  }

  /**
   * Calls the {@code getReadStream} method on the underlying client and returns an {@link
   * ADLFileInputStream} to read the file
   *
   * @throws IOException
   */
  public InputStream getReadStream(String testFile) throws IOException {
    return client.getReadStream(testFile);
  }

  /** Helper class for creating a {@link Request} object. */
  public static class AdlsRequestBuilder {

    // API version from ADLS SDK's HttpTransport class.
    // If you are upgrading the API_VERSION, please ensure that the error message parsing
    // in AdlsResponseHandler gets updated accordingly based on HttpTransport#getCodesFromJSon()
    // and ensure URI generation in build() is updated based on HttpTransport#makeSingleCall().
    private static final String API_VERSION = "2018-05-01";

    private final QueryParams params = new QueryParams();

    private final ADLStoreClient client;
    private final Logger logger;
    private final Map<String, List<String>> headers = new HashMap<>();

    private String filePath;

    public AdlsRequestBuilder(ADLStoreClient client, String authToken, Logger logger) {
      this.client = client;
      this.logger = logger;
      params.setApiVersion(API_VERSION);
      addHeader(HttpHeaderNames.HOST.toString(), client.getAccountName());
      addHeader("Authorization", authToken);
      addHeader("User-Agent", client.getUserAgent());
    }

    public AdlsRequestBuilder addQueryParam(String key, String value) {
      params.add(key, value);
      return this;
    }

    public AdlsRequestBuilder addHeader(String header, String value) {
      final List<String> headerValue = headers.get(header);
      if (headerValue != null) {
        headerValue.add(value);
      } else {
        headers.put(header, Arrays.asList(value));
      }
      return this;
    }

    public AdlsRequestBuilder setOp(Operation opt) {
      params.setOp(opt);
      return this;
    }

    public AdlsRequestBuilder setFilePath(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public Request build() {
      Preconditions.checkNotNull(filePath, "File path must be specified.");
      Preconditions.checkNotNull(params.op, "Operation must be specified.");

      final StringBuilder pathBuilder = new StringBuilder(params.op.namespace);
      final String prefix = client.getFilePathPrefix();
      if (prefix != null) {
        pathBuilder.append(prefix);
      }

      if (filePath.charAt(0) != '/') {
        pathBuilder.append('/');
      }
      pathBuilder.append(filePath);

      final URIBuilder uriBuilder =
          new URIBuilder()
              .setScheme(client.getHttpPrefix())
              .setHost(client.getAccountName())
              .setPath(pathBuilder.toString())
              .setCustomQuery(params.serialize());

      try {
        final String jdkUri = uriBuilder.build().toASCIIString();
        logger.debug("ADLS request built: {}", jdkUri);

        return new RequestBuilder().setUri(Uri.create(jdkUri)).setHeaders(headers).build();
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
