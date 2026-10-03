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
package com.dremio.plugins.icebergcatalog.store;

import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;
import java.util.List;
import javax.inject.Provider;
import javax.validation.constraints.NotBlank;

/**
 * Generic Iceberg REST Catalog source (e.g. Apache Polaris, or any server implementing the Iceberg
 * REST Catalog spec). Polaris is handled as a UI preset that stores this same type.
 */
@SourceType(
    value = "RESTCATALOG",
    label = "Iceberg REST Catalog",
    uiConfig = "restcatalog-layout.json")
public class RestIcebergCatalogPluginConfig extends IcebergCatalogPluginConfig {

  // 1-9   - IcebergCatalogPluginConfig
  // 10-19 - RestIcebergCatalogPluginConfig
  // 20-109 - Reserved by other plugins

  @NotBlank
  @Tag(10)
  @DisplayMetadata(label = "Endpoint URI")
  public String restEndpointUri;

  /**
   * List of allowed namespaces. Set to null by default to indicate that all namespaces are visible
   * Uses {@code com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR}
   * as NS separator regex sequence, by default "\\."
   */
  @Tag(11)
  @DisplayMetadata(label = "Allowed Namespaces")
  public List<String> allowedNamespaces;

  /**
   * When true (the default), the namespaces below each allowed namespace are discovered as well.
   * When false, only the tables and views directly in the allowed namespaces are listed.
   */
  @Tag(12)
  @DisplayMetadata(label = "Allowed Namespaces include their whole subtrees")
  public boolean isRecursiveAllowedNamespaces = true;

  /**
   * When true, the plugin asks the REST catalog to vend short-lived storage credentials by sending
   * {@code X-Iceberg-Access-Delegation: vended-credentials} (unless the user already set that
   * header in the catalog properties). Defaults to false so existing sources keep their behavior.
   * Toggling it only changes a request header, not the set of visible tables, so it does not
   * invalidate the source's dataset metadata.
   */
  @Tag(13)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Use vended credentials")
  public boolean isUsingVendedCredentials = false;

  public String getRestEndpointURI(DremioConfig dremioConfig) {
    return restEndpointUri;
  }

  @Override
  public IcebergCatalogPlugin newPlugin(
      PluginSabotContext pluginSabotContext,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new RestIcebergCatalogPlugin(this, pluginSabotContext, name, pluginIdProvider);
  }
}
