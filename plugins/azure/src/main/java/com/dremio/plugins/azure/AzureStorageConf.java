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

import static com.dremio.hadoop.security.alias.DremioCredentialProvider.PROTOCOL_PREFIX;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;

/** Azure Storage (including datalake v2) */
@CheckAzureConf
@SourceType(
    value = "AZURE_STORAGE",
    label = "Azure Storage",
    uiConfig = "azure-storage-layout.json")
public class AzureStorageConf extends AbstractAzureStorageConf {

  /** Secret Key <-> Azure Key Vault selector for 'Shared Access Key' authenticationType */
  @Tag(18)
  @DisplayMetadata(label = "Secret Store")
  public SharedAccessSecretType sharedAccessSecretType =
      SharedAccessSecretType.SHARED_ACCESS_SECRET_KEY;

  /** Secret Key <-> Azure Key Vault selector for 'Azure Active Directory' authenticationType */
  @Tag(19)
  @DisplayMetadata(label = "Application Secret Store")
  public AzureActiveDirectorySecretType azureADSecretType =
      AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_SECRET_KEY;

  /** vault uri for 'Shared Access Key' authenticationType */
  @Tag(20)
  @DisplayMetadata()
  public String accessKeyUri;

  /** vault uri for 'Azure Active Directory' authenticationType */
  @Tag(21)
  @DisplayMetadata()
  public String clientSecretUri;

  /**
   * @return the secret type (Secret Key 'dremio' vs. Azure Vault URI) for Shared Access
   *     authentication type
   */
  @Override
  public SharedAccessSecretType getSharedAccessSecretType() {
    return sharedAccessSecretType;
  }

  /**
   * @return the Shared Access Key Vault URI
   */
  @Override
  public String getAccessKeyUri() {
    accessKeyUri = prependProtocolIfNotExist(accessKeyUri, PROTOCOL_PREFIX);
    return accessKeyUri;
  }

  /**
   * @return the secret type (Secret Key 'dremio' vs. Azure Vault URI) for Azure Active Directory
   *     authentication type
   */
  @Override
  public AzureActiveDirectorySecretType getAzureADSecretType() {
    return azureADSecretType;
  }

  /**
   * @return the Azure Active Directory Key Vault URI
   */
  @Override
  public String getClientSecretUri() {
    clientSecretUri = prependProtocolIfNotExist(clientSecretUri, PROTOCOL_PREFIX);
    return clientSecretUri;
  }

  /**
   * @return the prepended uri (if protocol not yet present)
   */
  private String prependProtocolIfNotExist(String uri, String protocol) {
    return uri.toLowerCase().contains(protocol) ? uri : protocol.concat(uri);
  }
}
