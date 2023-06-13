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

import java.util.List;

import com.dremio.exec.catalog.conf.DisplayMetadata;

import io.protostuff.Tag;

/**
 * Authentication type for Azure Storage Accounts.
 * ACCESS_KEY uses credentials, AZURE_ACTIVE_DIRECTORY uses OAuth tokens.
 */

public enum AzureAuthenticationType {
  @Tag(1) @DisplayMetadata(label = "Shared access key")
  ACCESS_KEY {
    @Override
    public List<String> getUniqueProperties() {
      return AzureStorageConf.KEY_AUTH_PROPS;
    }
  },
  @Tag(2) @DisplayMetadata(label = "Azure Active Directory")
  AZURE_ACTIVE_DIRECTORY {
    @Override
    public List<String> getUniqueProperties() {
      return AzureStorageConf.AZURE_AD_PROPS;
    }
  };

  public abstract List<String> getUniqueProperties();
}
