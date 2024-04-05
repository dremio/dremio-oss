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

import com.dremio.common.exceptions.UserException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADToken;
import org.asynchttpclient.Request;

/**
 * Unlike {@link org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider}, this implementation
 * of {@link CustomTokenProviderAdaptee} renews the token after reloading the configuration (which
 * could reload the secret itself).
 */
public final class ClientCredentialsBasedTokenProviderImpl
    implements ClientCredentialsBasedTokenProvider {

  private static final String TOKEN_ENDPOINT = "/oauth2/token";
  private static final String INVALID_OAUTH_URL_ERROR_MESSAGE =
      "Invalid OAuth 2.0 Token Endpoint. Expected format is https://<host>/<tenantId>/oauth2/token";
  private static final long REFRESH_RANGE = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private Configuration configuration;

  private volatile AzureADToken token;

  public ClientCredentialsBasedTokenProviderImpl() {
    // hadoop uses reflection to create an instance; see AbfsConfiguration#getTokenProvider
  }

  @Override
  public void initialize(Configuration configuration, String accountName) throws IOException {
    this.configuration = configuration;

    validateOAuthURL(configuration.get(AzureStorageFileSystem.TOKEN_ENDPOINT));
    Preconditions.checkNotNull(configuration.get(AzureStorageFileSystem.CLIENT_ID));
    Preconditions.checkNotNull(configuration.getPassword(AzureStorageFileSystem.CLIENT_SECRET));
  }

  private void renewToken() throws IOException {
    final String authEndpoint = configuration.get(AzureStorageFileSystem.TOKEN_ENDPOINT);
    final String clientId = configuration.get(AzureStorageFileSystem.CLIENT_ID);
    final String clientSecret =
        new String(configuration.getPassword(AzureStorageFileSystem.CLIENT_SECRET));

    token = AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint, clientId, clientSecret);
  }

  private static boolean isNotCloseToExpiry(AzureADToken token) {
    return token.getExpiry().getTime() >= System.currentTimeMillis() + REFRESH_RANGE;
  }

  private boolean renewTokenIfNeeded() throws IOException {
    if (token != null && isNotCloseToExpiry(token)) {
      return false;
    }

    synchronized (this) {
      if (token != null && isNotCloseToExpiry(token)) {
        return false;
      }

      renewToken();
      return true;
    }
  }

  @Override
  public String getAccessToken() throws IOException {
    renewTokenIfNeeded();

    return token.getAccessToken();
  }

  @Override
  public Date getExpiryTime() {
    return token == null ? null : token.getExpiry();
  }

  @Override
  public String getAccessTokenUnchecked() {
    checkAndUpdateToken();

    return token.getAccessToken();
  }

  @Override
  public boolean checkAndUpdateToken() {
    try {
      return renewTokenIfNeeded();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getAuthzHeaderValue(Request req) {
    checkAndUpdateToken();

    return String.format("Bearer %s", token.getAccessToken());
  }

  @VisibleForTesting
  static void validateOAuthURL(String oauthUrl) throws IOException {
    if (oauthUrl == null || oauthUrl.isEmpty()) {
      throw UserException.validationError()
          .message("OAuth 2.0 Token Endpoint cannot be empty.")
          .buildSilently();
    }
    URL authorityUrl = new URL(oauthUrl);
    String host = authorityUrl.getAuthority().toLowerCase();
    if (authorityUrl.getPath().length() < 2 || authorityUrl.getPath().charAt(0) != '/') {
      throw UserException.validationError()
          .message(INVALID_OAUTH_URL_ERROR_MESSAGE)
          .buildSilently();
    }
    String path = authorityUrl.getPath().substring(1).toLowerCase();
    if (!path.contains("/")) {
      throw UserException.validationError()
          .message(INVALID_OAUTH_URL_ERROR_MESSAGE)
          .buildSilently();
    }
    String tenantId = path.substring(0, path.indexOf("/")).toLowerCase();
    String tokenEndpoint = "https://" + host + "/" + tenantId + TOKEN_ENDPOINT;
    if (!tokenEndpoint.equals(oauthUrl)) {
      throw UserException.validationError()
          .message(INVALID_OAUTH_URL_ERROR_MESSAGE)
          .buildSilently();
    }
  }
}
