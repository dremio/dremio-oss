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
package com.dremio.sabot.exec.fragment;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.sabot.driver.SchemaChangeListener;
import com.dremio.service.namespace.StoragePluginId;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Information required for some specialized scan purposes.
 */
public class FragmentExecutionContext {

  private final NodeEndpoint foreman;
  private final SchemaChangeListener schemaUpdater;
  private final StoragePluginRegistry registry;
  private final ListenableFuture<Boolean> cancelled;

  public FragmentExecutionContext(NodeEndpoint foreman, SchemaChangeListener schemaUpdater, StoragePluginRegistry registry, ListenableFuture<Boolean> cancelled) {
    super();
    this.foreman = foreman;
    this.schemaUpdater = schemaUpdater;
    this.registry = registry;
    this.cancelled = cancelled;
  }

  public NodeEndpoint getForemanEndpoint(){
    return foreman;
  }

  public SchemaChangeListener getSchemaUpdater(){
    return schemaUpdater;
  }

  public ListenableFuture<Boolean> cancelled() {
    return cancelled;
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T getStoragePlugin(StoragePluginId pluginId) throws ExecutionSetupException {
    StoragePlugin plugin = registry.getPlugin(pluginId);
    if(plugin == null){
      return null;
    }
    return (T) plugin;
  }
}
