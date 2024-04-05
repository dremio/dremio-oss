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

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestClientCredentialsBasedTokenProvider {

  @Test // move the test
  public void testOAuthURLValidation() {
    List<String> emptyOauthURLs = Arrays.asList(null, "");
    List<String> validOauthURLs =
        Arrays.asList(
            "https://login.microsoftonline.com/tenantid/oauth2/token",
            "https://www.microsoftonline.com/tenantid/oauth2/token");
    List<String> invalidOauthURLs =
        Arrays.asList(
            "http:///",
            "https:///",
            "https://login.microsoftonline.com/",
            "https://login.microsoftonline.com/tenantid",
            "https://login.microsoftonline.com/tenantid/",
            "https://login.microsoftonline.com/tenantid/oauth2",
            "https://login.microsoftonline.com/tenantid/oauth2.0/token",
            "http://login.microsoftonline.com/tenantid/oauth2/token",
            "https://login.microsoftonline.com/tenantid/oauth2/tokens");
    int emptyOauthURLsCount = 0;
    int validOauthURLsCount = 0;
    int invalidOauthURLsCount = 0;
    for (String url : emptyOauthURLs) {
      try {
        ClientCredentialsBasedTokenProviderImpl.validateOAuthURL(url);
      } catch (Exception exception) {
        emptyOauthURLsCount++;
        Assert.assertEquals(exception.getMessage(), "OAuth 2.0 Token Endpoint cannot be empty.");
      }
    }
    for (String url : validOauthURLs) {
      try {
        ClientCredentialsBasedTokenProviderImpl.validateOAuthURL(url);
        validOauthURLsCount++;
      } catch (Exception ignore) {
        System.out.println(ignore);
      }
    }
    for (String url : invalidOauthURLs) {
      try {
        ClientCredentialsBasedTokenProviderImpl.validateOAuthURL(url);
      } catch (Exception exception) {
        invalidOauthURLsCount++;
        Assert.assertEquals(
            exception.getMessage(),
            "Invalid OAuth 2.0 Token Endpoint. Expected format is https://<host>/<tenantId>/oauth2/token");
      }
    }
    Assert.assertEquals(emptyOauthURLsCount, emptyOauthURLs.size());
    Assert.assertEquals(validOauthURLsCount, validOauthURLs.size());
    Assert.assertEquals(invalidOauthURLsCount, invalidOauthURLs.size());
  }
}
