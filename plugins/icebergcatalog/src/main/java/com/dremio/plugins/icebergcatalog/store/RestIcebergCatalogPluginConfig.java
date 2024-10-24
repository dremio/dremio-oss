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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.iceberg.IcebergUtils;
import io.protostuff.Tag;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;

@SourceType(
    value = "RESTCATALOG",
    label = "REST Iceberg Catalog",
    uiConfig = "restcatalog-layout.json")
public class RestIcebergCatalogPluginConfig extends IcebergCatalogPluginConfig {

  // 1-9   - IcebergCatalogPluginConfig
  // 10-19 - RestIcebergCatalogPluginConfig
  // 20-99 - Reserved

  @Tag(10)
  @DisplayMetadata(label = "Endpoint URI")
  public String restEndpointUri;

  @Tag(11)
  @DisplayMetadata(label = "Allowed Namespaces")
  /**
   * List of allowed namespaces. Set to null by default to indicate that all namespaces are visible
   * Uses {@code com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR}
   * as NS separator regex sequence, by default "\\."
   */
  public List<String> allowedNamespaces;

  @Tag(12)
  @DisplayMetadata(label = "Allowed Namespaces include their whole subtrees")
  /**
   * List of allowed namespaces. Set to null by default to indicate that all namespaces are visible
   * Uses {@code com.dremio.exec.store.IcebergCatalogPluginOptions.RESTCATALOG_ALLOWED_NS_SEPARATOR}
   * as NS separator regex sequence, by default "\\."
   */
  public boolean isRecursiveAllowedNamespaces = true;

  @Override
  public CatalogAccessor createCatalog(Configuration config, SabotContext context) {
    try {
      initializeHadoopConf(config);
      return new RestCatalogAccessor(
          createRestCatalog(config, context),
          config,
          context.getOptionManager(),
          allowedNamespaces,
          isRecursiveAllowedNamespaces);
    } catch (Exception e) {
      throw UserException.connectionError(e)
          .message("Can't connect to %s catalog. %s", restCatalogImpl(context).getSimpleName(), e)
          .buildSilently();
    }
  }

  protected String catalogName() {
    return null;
  }

  protected Class<?> restCatalogImpl(SabotContext context) {
    return RESTCatalog.class;
  }

  protected Map<String, String> catalogProperties(SabotContext context) {
    Map<String, String> properties = new HashMap<>();

    String catalogImplClassName = restCatalogImpl(context).getName();
    properties.put(CatalogProperties.CATALOG_IMPL, catalogImplClassName);
    properties.put(CatalogProperties.URI, restEndpointUri);

    return properties;
  }

  protected com.google.common.base.Supplier<Catalog> createRestCatalog(
      Configuration config, SabotContext context) {
    Map<String, String> properties = catalogProperties(context);

    config.set(IcebergUtils.ENABLE_AZURE_ABFSS_SCHEME, "true");
    properties.put(IcebergUtils.ENABLE_AZURE_ABFSS_SCHEME, "true");

    for (Property p : getProperties()) {
      config.set(p.name, p.value);
      properties.put(p.name, p.value);
    }

    return () ->
        CatalogUtil.loadCatalog(
            restCatalogImpl(context).getName(), catalogName(), properties, config);
  }
}
