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
package com.dremio.plugins.elastic;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ReadLimitInfo;
import com.amazonaws.Request;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.http.HttpMethodName;
import com.amazonaws.util.AWSRequestMetrics;
import com.google.common.base.Joiner;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import org.glassfish.jersey.message.MessageBodyWorkers;
import org.glassfish.jersey.uri.UriComponent;

/**
 * Used to wrap up ClientRequestContext and AWS4Signer will add headers to the request for
 * signature.
 */
public class AWSRequest<T> implements Request<T> {
  /* the name of AWS service */
  private final String serviceName;
  /* original request context */
  private final ClientRequestContext clientRequestContext;
  /* Amazon web service request used to process content */
  private final AmazonWebServiceRequest originalAmazonWebServiceRequest =
      AmazonWebServiceRequest.NOOP;

  private final MessageBodyWorkers workers;

  public AWSRequest(
      String serviceName, ClientRequestContext clientRequestContext, MessageBodyWorkers workers) {
    this.serviceName = serviceName;
    this.clientRequestContext = clientRequestContext;
    this.workers = workers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setResourcePath(String path) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public Request<T> withParameter(String name, String value) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setParameters(Map<String, List<String>> parameters) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void addParameters(String name, List<String> values) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setEndpoint(URI endpoint) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setHttpMethod(HttpMethodName httpMethod) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  @Override
  public AmazonWebServiceRequest getOriginalRequest() {
    return originalAmazonWebServiceRequest;
  }

  @Override
  public void setTimeOffset(int timeOffset) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public Request<T> withTimeOffset(int timeOffset) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public AWSRequestMetrics getAWSRequestMetrics() {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setAWSRequestMetrics(AWSRequestMetrics metrics) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public <X> void addHandlerContext(HandlerContextKey<X> key, X value) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public <X> X getHandlerContext(HandlerContextKey<X> key) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void addHeader(String name, String value) {
    clientRequestContext.getHeaders().putSingle(name, value);
  }

  @Override
  public void addParameter(String name, String value) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public void setContent(InputStream content) {
    throw new UnsupportedOperationException("Not supported by AWSRequest");
  }

  @Override
  public Map<String, String> getHeaders() {
    return getHeadersMap(clientRequestContext.getStringHeaders());
  }

  @Override
  public String getResourcePath() {
    return clientRequestContext.getUri().getPath();
  }

  @Override
  public Map<String, List<String>> getParameters() {
    return UriComponent.decodeQuery(clientRequestContext.getUri(), true);
  }

  @Override
  public URI getEndpoint() {
    URI uri = clientRequestContext.getUri();
    try {
      return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "failed to set endpoint for aws request, url is " + uri.toString(), e);
    }
  }

  @Override
  public HttpMethodName getHttpMethod() {
    return HttpMethodName.valueOf(clientRequestContext.getMethod());
  }

  @Override
  public int getTimeOffset() {
    return 0;
  }

  @Override
  public InputStream getContent() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Object entity = clientRequestContext.getEntity();
    if (entity == null) {
      return null;
    } else {
      MessageBodyWriter messageBodyWriter =
          workers.getMessageBodyWriter(
              entity.getClass(),
              entity.getClass(),
              new Annotation[] {},
              clientRequestContext.getMediaType());
      try {
        // use the MBW to serialize entity into baos
        messageBodyWriter.writeTo(
            entity,
            entity.getClass(),
            entity.getClass(),
            new Annotation[] {},
            clientRequestContext.getMediaType(),
            new MultivaluedHashMap<String, Object>(),
            baos);
      } catch (IOException e) {
        throw new RuntimeException("Error while serializing entity.", e);
      }
      return new ByteArrayInputStream(baos.toByteArray());
    }
  }

  @Override
  public InputStream getContentUnwrapped() {
    return getContent();
  }

  @Override
  public ReadLimitInfo getReadLimitInfo() {
    return originalAmazonWebServiceRequest;
  }

  @Override
  public Object getOriginalRequestObject() {
    return null;
  }

  private Map<String, String> getHeadersMap(MultivaluedMap<String, String> m) {
    Map<String, String> map = new HashMap<String, String>();
    if (m == null) {
      return map;
    }
    for (Map.Entry<String, List<String>> entry : m.entrySet()) {
      map.put(entry.getKey(), Joiner.on(",").join(entry.getValue()));
    }
    return map;
  }
}
