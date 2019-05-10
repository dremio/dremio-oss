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
package com.dremio.service.namespace;

import java.io.IOException;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;

/**
 * Serializer for namespace container.
 */
final class NameSpaceContainerSerializer extends Serializer<NameSpaceContainer> {
  private final Serializer<NameSpaceContainer> serializer = ProtostuffSerializer.of(NameSpaceContainer.getSchema());

  public NameSpaceContainerSerializer() {
  }

  @Override
  public String toJson(NameSpaceContainer v) throws IOException {
    return serializer.toJson(v);
  }

  @Override
  public NameSpaceContainer fromJson(String v) throws IOException {
    return upgrade(serializer.fromJson(v));
  }

  @Override
  public byte[] convert(NameSpaceContainer v) {
    return serializer.convert(v);
  }

  @Override
  public NameSpaceContainer revert(byte[] v) {
    return upgrade(serializer.revert(v));
  }

  /**
   * In-line upgrade of a namespace container
   */
  private static NameSpaceContainer upgrade(NameSpaceContainer c) {
    if (NameSpaceContainer.Type.SOURCE.equals(c.getType()) &&
        c.getSource() != null) {
      return upgradeSource(c);
    }
    return c;
  }

  /**
   * In-line upgrade of a source-type namespace container
   */
  private static NameSpaceContainer upgradeSource(NameSpaceContainer c) {
    SourceConfig sourceConfig = c.getSource();
    if (sourceConfig.getMetadataPolicy() != null &&
        sourceConfig.getMetadataPolicy().getDatasetUpdateMode() != null &&
        sourceConfig.getMetadataPolicy().getDatasetUpdateMode().equals(UpdateMode.INLINE)) {
      sourceConfig.getMetadataPolicy().setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED);
    }
    return c;
  }
}
