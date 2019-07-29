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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;

/**
 * Utility class for generating OAuth tokens from ClientId and ClientSecrets
 */
public class AzureTokenGenerator {

  // From: https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app
  private static final String RESOURCE = "https://storage.azure.com/";

  private final AuthenticationContext authContext;
  private final ClientCredential credential;

  private AuthenticationResult authResult;
  private static final long REFRESH_RANGE = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  public AzureTokenGenerator(String oauthUrl, String clientId, String clientSecret) throws IOException {
    try {
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

  /**
   * Get method to return token
   */
  public String getToken() {
    return authResult.getAccessToken();
  }

  /**
   * Checks whether token is about to expire or is already expired
   * If it is either of the scenarios, method updates the token and returns true
   * Otherwise, the token remains unchanged and method returns false
   */
  public synchronized boolean checkAndUpdateToken() throws Exception {
    if (authResult == null || isTokenAboutToExpire()) {
      authResult = requestNewToken();
      return true;
    }
    return false;
  }

  @VisibleForTesting
  public synchronized void updateToken() throws Exception {
    authResult = requestNewToken();
  }

  /**
   * Requests a new token with stored credentials
   */
  private synchronized AuthenticationResult requestNewToken() throws Exception {
    return authContext.acquireToken(RESOURCE, credential, null).get();
  }

  /**
   * Determines whether token has expired or is within 5 minutes of expiration.
   */
  private boolean isTokenAboutToExpire() {
    return authResult.getExpiresOnDate().getTime() < System.currentTimeMillis() + REFRESH_RANGE;
  }
}
