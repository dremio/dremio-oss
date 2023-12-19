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

import org.apache.hadoop.fs.azurebfs.services.SharedKeySigner;

import com.microsoft.azure.storage.StorageCredentials;

/**
 * A convenience interface to tie various Azure Credentials objects together
 */
public interface AzureStorageCredentials extends SharedKeySigner, AzureAuthTokenProvider {

  /**
   * Export the credentials as {@link StorageCredentials}. These credentials should be treated as
   * a snapshot of the vaule at the time of calling. For situations where the secret may change
   * (i.e. secret rotation), the secret must be re-exported.
   */
  public StorageCredentials exportToStorageCredentials();
}
