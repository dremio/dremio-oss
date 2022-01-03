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

package com.dremio.plugins.azure.utils;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.util.CoreUtils;
import com.azure.core.util.DateTimeRfc1123;
import com.azure.core.util.UserAgentUtil;
import com.azure.storage.common.Utility;

/**
 * Prepares new instances of AsyncHttpClient
 */
public final class AzureAsyncHttpClientUtils {
  public static final String XMS_VERSION = "2019-07-07"; // represents version compatibility of the client.

  private static final Logger logger = LoggerFactory.getLogger(AzureAsyncHttpClientUtils.class);

  private static final Map<String, String> PROPERTIES = CoreUtils.getProperties("META-INF/maven/com.azure/azure-storage-common/pom.properties");
  private static final String sdkVersion = PROPERTIES.getOrDefault("version", "Unknown");
  private static final String USER_AGENT_VAL = UserAgentUtil.toUserAgentString(null, "azure-storage-blob", sdkVersion, null);

  private AzureAsyncHttpClientUtils() {
    // Not to be instantiated.
  }

  public static String getBaseEndpointURL(final String azureEndpoint, final String accountName, final boolean isSecure) {
    final String protocol = isSecure ? "https" : "http";
    return String.format("%s://%s.%s", protocol, accountName, azureEndpoint);
  }

  public static RequestBuilder newDefaultRequestBuilder() {
    return new RequestBuilder(HttpConstants.Methods.GET)
      .addHeader("Date", toHttpDateFormat(System.currentTimeMillis()))
      .addHeader("Content-Length", 0)
      .addHeader("x-ms-version", XMS_VERSION)
      .addHeader("x-ms-client-request-id", UUID.randomUUID().toString())
      .addHeader("User-Agent", USER_AGENT_VAL);
  }

  public static String encodeUrl(String raw) {
    return Utility.urlEncode(raw);
  }

  public static String toHttpDateFormat(final long timeInMillis) {
    final OffsetDateTime time = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeInMillis), ZoneId.systemDefault());
    final DateTimeRfc1123 dateTimeRfc1123 = new DateTimeRfc1123(time);
    return dateTimeRfc1123.toString();
  }
}
