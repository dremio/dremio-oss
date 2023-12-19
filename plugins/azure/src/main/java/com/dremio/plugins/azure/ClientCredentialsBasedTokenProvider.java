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

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

/**
 * Token provider that issue Azure AD tokens, which can be used by calls to Azure via the hadoop-azure library,
 * and also async http calls to Azure.
 */
public interface ClientCredentialsBasedTokenProvider extends CustomTokenProviderAdaptee, AzureAuthTokenProvider {

  /**
   * Obtain the access token that should be added to https connection's header.
   * <p>
   * Prefer this method over {@link #getAccessToken}, which is an internal contract with hadoop-azure library to load
   * the token. Also, this method does not throw a checked {@link IOException}, expect runtime exceptions instead.
   *
   * @return String containing the access token
   */
  String getAccessTokenUnchecked();

}
