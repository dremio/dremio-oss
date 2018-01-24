/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.logical.serialization.serializers;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;


public class StoragePluginSerializer extends Serializer<StoragePlugin> {
  private final StoragePluginRegistry registry;

  public StoragePluginSerializer(final StoragePluginRegistry registry) {
    this.registry = Preconditions.checkNotNull(registry, "registry is required");
  }

  @Override
  public void write(final Kryo kryo, final Output output, final StoragePlugin plugin) {
    kryo.writeClassAndObject(output, plugin.getId().getConfig());
  }

  @Override
  public StoragePlugin read(final Kryo kryo, final Input input, final Class<StoragePlugin> type) {
    final StoragePluginConfig config = (StoragePluginConfig) kryo.readClassAndObject(input);
    try {
      final StoragePlugin plugin = registry.getPlugin(config);
      kryo.reference(plugin);
      return plugin;
    } catch (ExecutionSetupException e) {
      throw new RuntimeException("unable to get plugin", e);
    }
  }
}
