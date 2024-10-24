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
package com.dremio.plugins.dataplane.store;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.nessiemetadata.cache.NessieDataplaneCaffeineCacheProvider;
import io.protostuff.Tag;
import java.util.List;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Connection configuration for Nessie source Plugin. */
@SourceType(value = "NESSIE", label = "Nessie", uiConfig = "nessie-layout.json", isVersioned = true)
public class NessiePluginConfig extends AbstractDataplanePluginConfig {
  private static final Logger logger = LoggerFactory.getLogger(NessiePluginConfig.class);

  @Tag(1)
  @DisplayMetadata(label = "Nessie endpoint URL")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String nessieEndpoint;

  @Tag(2)
  @Secret
  @DisplayMetadata(label = "Bearer token")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef nessieAccessToken;

  // Tags 3 through 7 are defined in AbstractDataplanePluginConfig

  @Tag(8)
  @DisplayMetadata(label = "Authentication method")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public AWSAuthenticationType credentialType;

  // Tags 9 through 12 are defined in AbstractDataplanePluginConfig

  // Tag 13 is reserved

  @Tag(14)
  @DisplayMetadata(label = "Nessie authentication type")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public NessieAuthType nessieAuthType = NessieAuthType.BEARER;

  @Tag(15)
  @DisplayMetadata(label = "AWS profile")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String awsProfile;

  @Tag(16)
  @DisplayMetadata(label = "Encrypt connection")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public boolean secure = true;

  // Tags 17 through 31 are defined in AbstractDataplanePluginConfig
  // Tags 32 through 36 are defined in AbstractLakehouseCatalogPluginConfig
  @Tag(37)
  @DisplayMetadata(label = "Client ID")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public String oauth2ClientId;

  @Tag(38)
  @Secret
  @DisplayMetadata(label = "Client Secret")
  @NotMetadataImpacting // Dataplane plugins don't have metadata refresh, so all properties are not
  // metadata impacting
  public SecretRef oauth2ClientSecret;

  @Tag(39)
  @DisplayMetadata(label = "OAuth2 Token Endpoint")
  @NotMetadataImpacting
  public String oauth2TokenEndpointURI;

  @Override
  public DataplanePlugin newPlugin(
      SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new NessiePlugin(
        this, context, name, pluginIdProvider, new NessieDataplaneCaffeineCacheProvider(), null);
  }

  @Override
  public List<Property> getProperties() {
    List<Property> properties = super.getProperties();
    properties.addAll(
        NessiePluginUtils.getCredentialProperties(
            secure,
            getStorageProvider(),
            credentialType,
            awsAccessKey,
            awsAccessSecret,
            assumedRoleARN,
            awsProfile));
    return properties;
  }

  NessieAuthType getNessieAuthType() {
    return nessieAuthType;
  }

  SecretRef getNessieAccessToken() {
    return nessieAccessToken;
  }

  SecretRef getOauth2ClientSecret() {
    return oauth2ClientSecret;
  }

  String getOauth2ClientId() {
    return oauth2ClientId;
  }

  String getOauth2TokenEndpointURI() {
    return oauth2TokenEndpointURI;
  }

  String getNessieEndpoint() {
    return nessieEndpoint;
  }

  @Override
  public String getSourceTypeName() {
    return "Nessie";
  }
}
