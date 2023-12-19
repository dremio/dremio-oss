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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.util.HttpConstants;
import org.junit.Before;
import org.junit.Test;

import com.azure.core.util.CoreUtils;
import com.azure.core.util.UserAgentUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.microsoft.azure.storage.core.Base64;

/**
 * Tests for AzureSharedKeyCredentials.
 * These credentials use the headers, URI and keys and creates a HMAC from all of these to prepare the token.
 */
public class TestAzureSharedKeyCredentials {
  private final String mockAccount = "mock-account";
  private final String mockKey = Base64.encode("mock-key".getBytes());
  private AzureSharedKeyCredentials credentials;

  @Before
  public void before() {
    credentials = new AzureSharedKeyCredentials(mockAccount, () -> mockKey);
    credentials.setSecret(new AzureSharedKeyCredentials.LookupResult(mockAccount, mockKey, 0));
  }

  @Test
  public void testGetAuthzHeaderValue() {
    String authzHeaderValue = credentials.getAuthzHeaderValue(prepareTestRequest());
    assertEquals("SharedKey mock-account:ZwovG4J+nCDc3w58WPei6fvJBQsO96YojteJncy0wwI=", authzHeaderValue);
  }

  @Test
  public void testCheckAndUpdate() {
    // Test credentials are initialized with invalid secret
    assertTrue("credentials were not updated", credentials.checkAndUpdateToken());
  }

  @Test
  public void testNotExpired() {
    Ticker ticker = new Ticker() {
      private final long millis = System.currentTimeMillis()
        - AzureSharedKeyCredentials.SECRET_TTL + 100;
      @Override
      public long read() {
        return millis;
      }
    };

    credentials.setSecret(new AzureSharedKeyCredentials.LookupResult(
      mockAccount, mockKey, ticker.read()));

    assertFalse(credentials.checkAndUpdateToken());
  }

  @Test
  public void testExpired() {
    Ticker ticker = new Ticker() {
      private final long millis = System.currentTimeMillis()
        - AzureSharedKeyCredentials.SECRET_TTL - 100;
      @Override
      public long read() {
        return millis;
      }
    };

    credentials.setSecret(new AzureSharedKeyCredentials.LookupResult(
      mockAccount, mockKey, ticker.read()));

    assertTrue(credentials.checkAndUpdateToken());
  }

  @Test
  public void testCredentialsUpdate() {
    // Test credentials are updated and correct
    assertEquals("credentials were not updated or do not match", mockKey, credentials.getSecret().getSecret());
  }

  private Request prepareTestRequest() {
    final Map<String, String> properties = CoreUtils.getProperties("META-INF/maven/com.azure/azure-storage-common/pom.properties");
    final String sdkVersion = properties.getOrDefault("version", "Unknown");
    Preconditions.checkArgument(!"Unknown".equalsIgnoreCase(sdkVersion), "SDK Version cannot be unknown.");
    return new RequestBuilder(HttpConstants.Methods.GET)
      .addHeader("Date", "Tue, 31 Dec 2019 07:18:50 GMT")
      .addHeader("Content-Length", 0)
      .addHeader("x-ms-version", "2019-02-02")
      .addHeader("x-ms-client-request-id", "b2a11e2a-65a7-48ed-a643-229255139452")
      .addHeader("User-Agent", UserAgentUtil.toUserAgentString(null, "azure-storage-blob", sdkVersion, null))
      .addHeader("x-ms-range", String.format("bytes=%d-%d", 25, 125))
      .addHeader("If-Unmodified-Since", "Tue, 15 Dec 2019 07:18:50 GMT")
      .setUrl("https://account.blob.core.windows.net/container/directory%2Ffile_00.parquet")
      .build();
  }
}
