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
package com.dremio.exec.store.cache;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.io.file.FileBlockLocation;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocations;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DataFileUID;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utility to serialize and deserialize keys and values
 */
public class KeyValueSerDe {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueSerDe.class);

  public static byte[] serializeKey(String dataFilePath, String pluginId) {
    Objects.requireNonNull(dataFilePath);
    Objects.requireNonNull(pluginId);

    return DataFileUID.newBuilder()
      .setDataFilePath(dataFilePath)
      .setPluginId(pluginId)
      .build()
      .toByteArray();
  }

  public static DataFileUID deserializeKey(byte[] serialized) {
    Objects.requireNonNull(serialized);

    try {
      return DataFileUID.parseFrom(serialized);
    } catch (InvalidProtocolBufferException e) {
      LOGGER.debug("Deserialization of key failed", e);
      return null;
    }
  }

  public static byte[] serializeValue(Iterable<FileBlockLocation> blockLocations) {
    Objects.requireNonNull(blockLocations);

    BlockLocationsList.Builder blockLocationsBuilder = BlockLocationsList.newBuilder();
    for (FileBlockLocation blockLocation : blockLocations) {
      BlockLocations blockLocationProtobuf = BlockLocations.newBuilder()
        .setOffset(blockLocation.getOffset())
        .setSize(blockLocation.getSize())
        .addAllHosts(blockLocation.getHosts())
        .build();

      blockLocationsBuilder.addBlockLocations(blockLocationProtobuf);
    }
    return blockLocationsBuilder.build().toByteArray();
  }

  public static BlockLocationsList deserializeValue(byte[] serialized) {
    Objects.requireNonNull(serialized);

    try {
      return BlockLocationsList.parseFrom(serialized);
    } catch (InvalidProtocolBufferException e) {
      LOGGER.debug("Deserialization of value failed", e);
      return null;
    }
  }
}
