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
package com.dremio.exec.catalog;


import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreCreationFunction;
import com.dremio.datastore.api.LegacyStoreBuildingFactory;
import com.dremio.datastore.format.Format;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceInternalData;

/**
 * Creator for catalog source data kvstore
 */
public class CatalogSourceDataCreator implements LegacyKVStoreCreationFunction<NamespaceKey, SourceInternalData> {
  @Override
  public LegacyKVStore<NamespaceKey, SourceInternalData> build(LegacyStoreBuildingFactory factory) {
    return factory.<NamespaceKey, SourceInternalData>newStore()
      .name(CatalogServiceImpl.CATALOG_SOURCE_DATA_NAMESPACE)
      .keyFormat(Format.wrapped(NamespaceKey.class, NamespaceKey::toString, NamespaceKey::new, Format.ofString()))
      .valueFormat(Format.ofProtostuff(SourceInternalData.class))
      .versionExtractor(SourceInternalDataVersionExtractor.class)
      .build();
  }

  /**
   * version extractor for namespace container.
   */
  public static class SourceInternalDataVersionExtractor implements VersionExtractor<SourceInternalData> {
    @Override
    public String getTag(SourceInternalData value) {
      return value.getTag();
    }

    @Override
    public void setTag(SourceInternalData value, String tag) {
      value.setTag(tag);
    }

    @Override
    public Long getVersion(SourceInternalData value) {
      return value.getVersion();
    }

    @Override
    public void setVersion(SourceInternalData value, Long version) {
      value.setVersion(version);
    }
  }
}
