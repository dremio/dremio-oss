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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.asynchttpclient.Request;

import com.azure.storage.common.StorageSharedKeyCredential;

/**
 * Shared access key based token provider
 */
public class AzureSharedKeyAuthTokenProvider implements AzureAuthTokenProvider {
  private final StorageSharedKeyCredential storageSharedKeyCredential;

  public AzureSharedKeyAuthTokenProvider(final String accountName, final String key) {
    this.storageSharedKeyCredential = new StorageSharedKeyCredential(accountName, key);
  }

  @Override
  public boolean checkAndUpdateToken() {
    // Shared key never changes
    return false;
  }

  @Override
  public String getAuthzHeaderValue(final Request req) {
    try {
      final URL url = new URL(req.getUrl());
      final Map<String, String> headersMap = new HashMap<>();
      req.getHeaders().forEach(header -> headersMap.put(header.getKey(), header.getValue()));
      return this.storageSharedKeyCredential.generateAuthorizationHeader(url, req.getMethod(), headersMap);
    } catch (MalformedURLException e) {
      throw new IllegalStateException("The request URL is invalid ", e);
    }
  }

  @Override
  public boolean isCloseToExpiry() {
    return false;
  }
}
