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

import static com.dremio.hadoop.security.alias.DremioCredentialProvider.DREMIO_SCHEME_PREFIX;
import static com.dremio.hadoop.security.alias.DremioCredentialProvider.PROTOCOL_PREFIX;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.DoNotDisplay;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.service.namespace.AbstractConnectionConf;
import io.protostuff.Tag;
import org.apache.commons.lang3.StringUtils;

/** Azure Storage (including datalake v2) */
@CheckAzureConf
@SourceType(
    value = "AZURE_STORAGE",
    label = "Azure Storage",
    uiConfig = "azure-storage-layout.json")
public class AzureStorageConf extends AbstractAzureStorageConf {
  private static final String AZURE_VAULT_PREFIX = "azure-key-vault+";

  /** Secret Key <-> Azure Key Vault selector for 'Shared Access Key' authenticationType */
  @Tag(18)
  @DisplayMetadata(label = "Secret Store")
  @DoNotDisplay // Soft Deprecated
  public SharedAccessSecretType sharedAccessSecretType =
      SharedAccessSecretType.SHARED_ACCESS_SECRET_KEY;

  /** Secret Key <-> Azure Key Vault selector for 'Azure Active Directory' authenticationType */
  @Tag(19)
  @DisplayMetadata(label = "Application Secret Store")
  @DoNotDisplay // Soft Deprecated
  public AzureActiveDirectorySecretType azureADSecretType =
      AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_SECRET_KEY;

  /** vault uri for 'Shared Access Key' authenticationType */
  @Tag(20)
  @DisplayMetadata()
  @DoNotDisplay // Soft Deprecated
  public String accessKeyUri;

  /** vault uri for 'Azure Active Directory' authenticationType */
  @Tag(21)
  @DisplayMetadata()
  @DoNotDisplay // Soft Deprecated
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
    accessKeyUri = prependProtocolIfNotExist(accessKeyUri);
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
    clientSecretUri = prependProtocolIfNotExist(clientSecretUri);
    return clientSecretUri;
  }

  /**
   * Handles deprecation of accessKeyUri and clientSecretUri for create/upgrade scenario. Removes
   * old uses/formats and repacks them into accessKey or clientSecret. Changes made here persist
   * into the KV store.
   *
   * @return true if changes made, otherwise false
   */
  @Override
  // TODO: remove once v25 no longer supported
  public boolean migrateLegacyFormat() {
    // Logic here is based on old AzureStoragePlugin#getProperties conditionals.

    // Handle Shared Access Case
    if (AzureAuthenticationType.ACCESS_KEY == credentialsType
        && SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT == sharedAccessSecretType
        && StringUtils.isNotBlank(accessKeyUri)) {
      accessKey = SecretRef.of(fixAzureKeyVaultPrefix(getAccessKeyUri()));
      accessKeyUri = null;
      sharedAccessSecretType = null;
      return true;
    }

    // Handle Entra ID / Active Directory Case
    if (AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY == credentialsType
        && AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT == azureADSecretType
        && StringUtils.isNotBlank(clientSecretUri)) {
      clientSecret = SecretRef.of(fixAzureKeyVaultPrefix(getClientSecretUri()));
      clientSecretUri = null;
      azureADSecretType = null;
      return true;
    }

    return false;
  }

  /**
   * Handles deprecation of accessKeyUri and clientSecretUri for source update scenarios. Ensures
   * new format takes priority over the old format. Changes made here persist into the KV store.
   */
  @Override
  // TODO: remove in v26
  public void migrateLegacyFormat(AbstractConnectionConf existingConf) {
    AzureStorageConf existingAzureConf = (AzureStorageConf) existingConf;

    // Handle Shared Access Case
    if (AzureAuthenticationType.ACCESS_KEY == credentialsType
        && !SecretRef.isNullOrEmpty(accessKey)
        && !accessKey.equals(existingAzureConf.accessKey)) {
      // Assume if accessKey is set and is new, that any old/deprecated fields are incorrect.
      accessKeyUri = null;
      sharedAccessSecretType = null;
      return;
    }

    // Handle Entra ID / Active Directory Case
    if (AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY == credentialsType
        && !SecretRef.isNullOrEmpty(clientSecret)
        && !clientSecret.equals(existingAzureConf.clientSecret)) {
      // Assume if clientSecret is set and is new, that any old/deprecated fields are incorrect.
      clientSecretUri = null;
      azureADSecretType = null;
      return;
    }

    // Otherwise delegate to create/upgrade logic.
    migrateLegacyFormat();
  }

  /** Back-fills accessKeyUri and clientSecretUri for use in API responses. */
  @Override
  // TODO: remove in v26
  public void backFillLegacyFormat() {
    // Handle Shared Access Case
    if (AzureAuthenticationType.ACCESS_KEY == credentialsType
        && !SecretRef.isNullOrEmpty(accessKey)) {
      final String displayString = SecretRef.getDisplayString(accessKey);
      if (displayString.startsWith(AZURE_VAULT_PREFIX)) {
        accessKeyUri = displayString.substring(AZURE_VAULT_PREFIX.length());
        sharedAccessSecretType = SharedAccessSecretType.SHARED_ACCESS_AZURE_KEY_VAULT;
      }
    }

    // Handle Entra ID / Active Directory Case
    if (AzureAuthenticationType.AZURE_ACTIVE_DIRECTORY == credentialsType
        && !SecretRef.isNullOrEmpty(clientSecret)) {
      final String displayString = SecretRef.getDisplayString(clientSecret);
      if (displayString.startsWith(AZURE_VAULT_PREFIX)) {
        clientSecretUri = displayString.substring(AZURE_VAULT_PREFIX.length());
        azureADSecretType = AzureActiveDirectorySecretType.AZURE_ACTIVE_DIRECTORY_KEY_VAULT;
      }
    }
  }

  /**
   * @return the prepended uri (if protocol not yet present)
   */
  private String prependProtocolIfNotExist(String uri) {
    if (StringUtils.isBlank(uri)) {
      return uri;
    }

    return uri.toLowerCase().contains(PROTOCOL_PREFIX) ? uri : PROTOCOL_PREFIX.concat(uri);
  }

  /** Converts input into the format: "azure-key-vault+https://some.vault.url". */
  private String fixAzureKeyVaultPrefix(String input) {
    // remove dremio+ from the input if it exists
    if (input.toLowerCase().startsWith(DREMIO_SCHEME_PREFIX)) {
      input = input.substring(DREMIO_SCHEME_PREFIX.length());
    }

    // return as is if valid.
    if (input.toLowerCase().startsWith(AZURE_VAULT_PREFIX)) {
      return input;
    }

    // add the azure key vault prefix if missing
    return AZURE_VAULT_PREFIX.concat(input);
  }
}
