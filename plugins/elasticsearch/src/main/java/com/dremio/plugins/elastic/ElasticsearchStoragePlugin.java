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
package com.dremio.plugins.elastic;

import java.io.IOException;
import java.util.List;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AbstractStoragePlugin;
import com.dremio.exec.store.ConversionContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin2;

/**
 * Shell for Elasticsearch storage plugin.
 */
public class ElasticsearchStoragePlugin extends AbstractStoragePlugin<ConversionContext.NamespaceConversionContext> {

  private final ElasticsearchStoragePluginConfig config;
  private final ElasticsearchStoragePlugin2 plugin2;

  public ElasticsearchStoragePlugin(ElasticsearchStoragePluginConfig config, SabotContext context, String name) {
    this.config = config;
    this.plugin2 = new ElasticsearchStoragePlugin2(config, context, name);
  }

  @Override
  public ElasticsearchStoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public StoragePlugin2 getStoragePlugin2() {
    return plugin2;
  }

  @Override
  public void start() throws IOException {
    plugin2.start();
  }

  @Override
  public void close() throws Exception {
    plugin2.close();
  }

  @Override
  public boolean folderExists(SchemaConfig schemaConfig, List<String> folderPath) throws IOException {
    // should never be called.
    return false;
  }

}
