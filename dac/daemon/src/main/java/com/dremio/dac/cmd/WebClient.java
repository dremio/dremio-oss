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
package com.dremio.dac.cmd;

import static java.lang.String.format;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Optional;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.eclipse.jetty.http.HttpHeader;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import com.dremio.config.DremioConfig;
import com.dremio.dac.model.usergroup.UserLogin;
import com.dremio.dac.model.usergroup.UserLoginSession;
import com.dremio.dac.server.DACConfig;
import com.dremio.dac.server.GenericErrorMessage;
import com.dremio.dac.server.tokens.TokenUtils;
import com.dremio.dac.util.JSONUtil;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.ssl.SSLHelper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * A web client to access api v2
 */
public class WebClient {
  private WebTarget target;
  private UserLoginSession userSession;
  private final boolean checkCertificates;

  public WebClient(DACConfig dacConfig, String userName, String password, boolean checkCertificates)
    throws IOException, GeneralSecurityException {
    this.checkCertificates = checkCertificates;


    this.target = getWebClient(dacConfig);
    this.userSession = buildPost(UserLoginSession.class, "/login", new UserLogin(userName, password), false);
  }

  public final <TResult, TRequest> TResult buildPost(Class<TResult> outputType, String path, TRequest requestBody)
    throws IOException {
    return buildPost(outputType, path, requestBody, true);
  }

  private final <TResult, TRequest> TResult buildPost(Class<TResult> outputType, String path,
    TRequest requestBody, boolean includeAuthToken) throws IOException {
    Invocation.Builder builder =  target.path(path).request(MediaType.APPLICATION_JSON_TYPE);

    if (includeAuthToken) {
      builder = builder.header(HttpHeader.AUTHORIZATION.toString(),
        TokenUtils.AUTH_HEADER_PREFIX + userSession.getToken());
    }

    return readEntity(outputType, builder.buildPost(Entity.json(requestBody)));
  }

  private WebTarget getWebClient(
    DACConfig dacConfig) throws IOException, GeneralSecurityException {
    final JacksonJaxbJsonProvider provider = new JacksonJaxbJsonProvider();
    provider.setMapper(JSONUtil.prettyMapper());
    ClientBuilder clientBuilder = ClientBuilder.newBuilder()
      .register(provider)
      .register(MultiPartFeature.class);

    if (dacConfig.webSSLEnabled()) {
      this.setTrustStore(clientBuilder, dacConfig);
    }

    final Client client = clientBuilder.build();
    return client.target(format("%s://%s:%d", dacConfig.webSSLEnabled() ? "https" : "http", dacConfig.thisNode,
      dacConfig.getHttpPort())).path("apiv2");
  }

  private void setTrustStore(ClientBuilder clientBuilder, DACConfig dacConfig)
    throws IOException, GeneralSecurityException {
    Optional<KeyStore> trustStore = Optional.empty();

    if (checkCertificates) {
      trustStore = new SSLConfigurator(dacConfig.getConfig(), DremioConfig.WEB_SSL_PREFIX, "web").getTrustStore();
      if (trustStore.isPresent()) {
        clientBuilder.trustStore(trustStore.get());
      }
    } else {
      SSLContext sslContext = SSLHelper.newAllTrustingSSLContext("SSL");
      HostnameVerifier verifier = SSLHelper.newAllValidHostnameVerifier();
      clientBuilder.hostnameVerifier(verifier);
      clientBuilder.sslContext(sslContext);
    }
  }

  private <T> T readEntity(Class<T> entityClazz, Invocation invocation) throws IOException {
    Response response = invocation.invoke();
    try {
      response.bufferEntity();
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        if (response.hasEntity()) {
          // Try to parse error message as generic error message JSON type
          try {
            GenericErrorMessage message = response.readEntity(GenericErrorMessage.class);
            throw new IOException(format("Status %d (%s): %s (more info: %s)",
              response.getStatus(),
              response.getStatusInfo().getReasonPhrase(),
              message.getErrorMessage(),
              message.getMoreInfo()));
          } catch (ProcessingException e) {
            // Fallback to String if unparsing is unsuccessful
            throw new IOException(format("Status %d (%s)",
              response.getStatus(),
              response.getStatusInfo().getReasonPhrase(),
              response.readEntity(String.class)));
          }
        }
        throw new IOException(format("Status %d (%s)",
          response.getStatus(),
          response.getStatusInfo().getReasonPhrase()));
      }
      return response.readEntity(entityClazz);
    } finally {
      response.close();
    }
  }
}
