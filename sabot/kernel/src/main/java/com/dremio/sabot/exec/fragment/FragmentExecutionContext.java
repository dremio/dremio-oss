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
package com.dremio.sabot.exec.fragment;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Information required for some specialized scan purposes.
 */
public class FragmentExecutionContext {

  private final NodeEndpoint foreman;
  private final CatalogService sources;
  private final ListenableFuture<Boolean> cancelled;
  private final CoordExecRPC.QueryContextInformation queryContextInformation;

  public FragmentExecutionContext(NodeEndpoint foreman, CatalogService sources, ListenableFuture<Boolean> cancelled, CoordExecRPC.QueryContextInformation context) {
    super();
    this.foreman = foreman;
    this.sources = sources;
    this.cancelled = cancelled;
    this.queryContextInformation = context;
  }

  public NodeEndpoint getForemanEndpoint(){
    return foreman;
  }

  public ListenableFuture<Boolean> cancelled() {
    return cancelled;
  }

  @SuppressWarnings("unchecked")
  public <T extends StoragePlugin> T getStoragePlugin(StoragePluginId pluginId) throws ExecutionSetupException {
    StoragePlugin plugin = sources.getSource(pluginId);
    if(plugin == null){
      return null;
    }
    return (T) plugin;
  }

  public CoordExecRPC.QueryContextInformation getQueryContextInformation() {
    return queryContextInformation;
  }
}
