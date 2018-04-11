/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;

import com.dremio.datastore.KVStore;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.StoreBuildingFactory;
import com.dremio.datastore.StoreCreationFunction;
import com.dremio.datastore.StringSerializer;
import com.dremio.datastore.VersionExtractor;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceInternalData;

/**
 * Creator for catalog source data kvstore
 */
public class CatalogSourceDataCreator implements StoreCreationFunction<KVStore<NamespaceKey, SourceInternalData>> {
  @Override
  public KVStore<NamespaceKey, SourceInternalData> build(StoreBuildingFactory factory) {
    return factory.<NamespaceKey, SourceInternalData>newStore()
      .name(CatalogServiceImpl.CATALOG_SOURCE_DATA_NAMESPACE)
      .keySerializer(NamespaceKeySerializer.class)
      .valueSerializer(SourceInternalDataSerializer.class)
      .versionExtractor(SourceInternalDataVersionExtractor.class)
      .build();
  }

  /**
   * version extractor for namespace container.
   */
  public static class SourceInternalDataVersionExtractor implements VersionExtractor<SourceInternalData> {
    @Override
    public Long getVersion(SourceInternalData value) {
      return value.getVersion();
    }
    @Override
    public void setVersion(SourceInternalData value, Long version) {
      value.setVersion(version);
    }
    @Override
    public Long incrementVersion(SourceInternalData value) {
      Long version = getVersion(value);
      setVersion(value, version == null ? 0 : version + 1);
      return version;
    }
  }

  /**
  * A serializer for namespace keys
  */
  public static class NamespaceKeySerializer extends Serializer<NamespaceKey> {
    @Override
    public String toJson(NamespaceKey v) throws IOException {
      return StringSerializer.INSTANCE.toJson(v.toString());
    }

    @Override
    public NamespaceKey fromJson(String v) throws IOException {
      return new NamespaceKey(StringSerializer.INSTANCE.fromJson(v));
    }

    @Override
    public byte[] convert(NamespaceKey v) {
      return StringSerializer.INSTANCE.convert(v.toString());
    }

    @Override
    public NamespaceKey revert(byte[] v) {
      return new NamespaceKey(StringSerializer.INSTANCE.revert(v));
    }
  }

  /**
   * Serializer for SourceInternalData.
   */
  public static class SourceInternalDataSerializer extends Serializer<SourceInternalData> {
    private final Serializer<SourceInternalData> serializer = ProtostuffSerializer.of(SourceInternalData.getSchema());

    public SourceInternalDataSerializer() {
    }

    @Override
    public String toJson(SourceInternalData v) throws IOException {
      return serializer.toJson(v);
    }

    @Override
    public SourceInternalData fromJson(String v) throws IOException {
      return serializer.fromJson(v);
    }

    @Override
    public byte[] convert(SourceInternalData v) {
      return serializer.convert(v);
    }

    @Override
    public SourceInternalData revert(byte[] v) {
      return serializer.revert(v);
    }
  }
}
