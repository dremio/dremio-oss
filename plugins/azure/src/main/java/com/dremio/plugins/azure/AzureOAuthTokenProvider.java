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

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.asynchttpclient.Request;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;

/**
 * Utility class for generating OAuth tokens from ClientId and ClientSecrets
 */
public class AzureOAuthTokenProvider implements AzureAuthTokenProvider {

  // From: https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app
  private static final String RESOURCE = "https://storage.azure.com/";
  private static final String TOKEN_ENDPOINT = "/oauth2/token";
  private static final String INVALID_OAUTH_URL_ERROR_MESSAGE = "Invalid OAuth 2.0 Token Endpoint. Expected format is https://<host>/<tenantId>/oauth2/token";

  private final AuthenticationContext authContext;
  private final ClientCredential credential;

  private AuthenticationResult authResult;
  private static final long REFRESH_RANGE = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  public AzureOAuthTokenProvider(String oauthUrl, String clientId, String clientSecret) throws IOException {
    try {
      // Microsoft's authentication expect tokenEndpoint to be in following format : https://<host>/<tenantId>/oauth2/token
      // Hence, validating the format before validation
      validateOAuthURL(oauthUrl);
      authContext = new AuthenticationContext(oauthUrl, true,
        Executors.newCachedThreadPool(new NamedThreadFactory("adls-oauth-request")));
      credential = new ClientCredential(clientId, clientSecret);
      authResult = requestNewToken();
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  public static void validateOAuthURL(String oauthUrl) throws Exception{
    if (oauthUrl == null || oauthUrl.isEmpty()) {
      throw UserException.validationError().message("OAuth 2.0 Token Endpoint cannot be empty.").buildSilently();
    }
    URL authorityUrl = new URL(oauthUrl);
    String host = authorityUrl.getAuthority().toLowerCase();
    if (authorityUrl.getPath().length() < 2 || authorityUrl.getPath().charAt(0) != '/') {
      throw UserException.validationError().message(INVALID_OAUTH_URL_ERROR_MESSAGE).buildSilently();
    }
    String path = authorityUrl.getPath().substring(1) .toLowerCase();
    if (!path.contains("/")) {
      throw UserException.validationError().message(INVALID_OAUTH_URL_ERROR_MESSAGE).buildSilently();
    }
    String tenantId = path.substring(0, path.indexOf("/")) .toLowerCase();
    String tokenEndpoint = "https://" + host + "/" + tenantId + TOKEN_ENDPOINT;
    if (!tokenEndpoint.equals(oauthUrl)) {
      throw UserException.validationError().message(INVALID_OAUTH_URL_ERROR_MESSAGE).buildSilently();
    }
  }

  public String getToken() {
    //to make sure token is never expired before returning it.
    checkAndUpdateToken();
    return authResult.getAccessToken();
  }

  @Override
  public String getAuthzHeaderValue(Request req) {
    return String.format("Bearer %s", getToken());
  }

  @Override
  public synchronized boolean checkAndUpdateToken() {
    try {
      if (authResult == null || isCloseToExpiry()) {
        authResult = requestNewToken();
        return true;
      }
      return false;
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Error while acquiring new access token from Azure", e);
    }
  }

  /**
   * Requests a new token with stored credentials
   */
  private synchronized AuthenticationResult requestNewToken() throws ExecutionException, InterruptedException {
    return authContext.acquireToken(RESOURCE, credential, null).get();
  }

  @Override
  public boolean isCloseToExpiry() {
    return authResult.getExpiresOnDate().getTime() < System.currentTimeMillis() + REFRESH_RANGE;
  }
}
