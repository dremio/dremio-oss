/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.utils.URIBuilder;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;

import com.google.common.base.Preconditions;

import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * Helper class for creating
 */
class AdlsRequestBuilder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AdlsRequestBuilder.class);

  // API version from ADLS SDK's HttpTransport class.
  // If you are upgrading the API_VERSION, please ensure that the error message parsing
  // in AdlsResponseHandler gets updated accordingly based on HttpTransport#getCodesFromJSon()
  // and ensure URI generation in build() is updated based on HttpTransport#makeSingleCall().
  private static final String API_VERSION = "2018-05-01";

  private final QueryParams params = new QueryParams();

  private final ADLStoreClient client;
  private final Map<String, List<String>> headers = new HashMap<>();

  private String filePath;

  public AdlsRequestBuilder(ADLStoreClient client, String authToken) {
    this.client = client;
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
    List<String> headerValue = headers.get(header);
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

    final URIBuilder uriBuilder = new URIBuilder()
      .setScheme(client.getHttpPrefix())
      .setHost(client.getAccountName())
      .setPath(pathBuilder.toString())
      .setCustomQuery(params.serialize());

    try {
      final String jdkUri = uriBuilder.build().toASCIIString();
      logger.debug("ADLS request built: {}", jdkUri);

      return new RequestBuilder()
        .setUri(Uri.create(jdkUri))
        .setHeaders(headers)
        .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
