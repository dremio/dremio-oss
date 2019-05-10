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
package com.dremio.dac.cmd.upgrade;

import com.dremio.common.Version;
import com.dremio.dac.cmd.AdminLogger;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.store.hive.exec.HiveReaderProtoUtil;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.hive.proto.HiveReaderProto.HiveTableXattr;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

import io.protostuff.ByteString;

/**
 * Rewrite hive dataset config entries to compress the read definitions
 */
class CompressHiveTableAttrs extends UpgradeTask implements LegacyUpgradeTask {

  //DO NOT MODIFY
  static final String taskUUID = "285abdab-19b9-493b-8c2e-f2759c6bbd04";

  public CompressHiveTableAttrs() {
    super("Compressing Hive Table attributes", ImmutableList.of(MigrateAccelerationMeasures.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_203;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());
    try {
      for (SourceConfig source : namespaceService.getSources()) {
        if (!"HIVE".equalsIgnoreCase(ConnectionReader.toType(source))) {
          continue;
        }

        AdminLogger.log("  Handling Hive source {}", source.getName());

        for (NamespaceKey datasetPath : namespaceService.getAllDatasets(new NamespaceKey(source.getName()))) {
          final DatasetConfig datasetConfig = namespaceService.getDataset(datasetPath);

          // protect against missing fields
          if (datasetConfig.getReadDefinition() == null || datasetConfig.getReadDefinition().getExtendedProperty() == null) {
            continue;
          }

          AdminLogger.log("    Compressing Table {}...", datasetPath.getSchemaPath());

          final ReadDefinition readDefinition = datasetConfig.getReadDefinition();
          final byte[] original = readDefinition.getExtendedProperty().toByteArray();

          final HiveTableXattr.Builder extended = HiveTableXattr.newBuilder(HiveTableXattr.parseFrom(original));
          if (extended.getPropertyCollectionType() == HiveReaderProto.PropertyCollectionType.DICTIONARY) {
            AdminLogger.log("already compressed, skipping");
            continue;
          }

          // compress the table's attributes
          HiveReaderProtoUtil.encodePropertiesAsDictionary(extended);
          final byte[] compressed = extended.build().toByteArray();
          readDefinition.setExtendedProperty(ByteString.copyFrom(compressed));

          final int ratio = 100 * compressed.length / original.length;

          AdminLogger.log("compressed {} -> {} {}", original.length, compressed.length, ratio);

          namespaceService.addOrUpdateDataset(datasetPath, datasetConfig);
        }
      }
    } catch (NamespaceException | InvalidProtocolBufferException e) {
      throw new RuntimeException("CompressHiveTableAttrs failed", e);
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
