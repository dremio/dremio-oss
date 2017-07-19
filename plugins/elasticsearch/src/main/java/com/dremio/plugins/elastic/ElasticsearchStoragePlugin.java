/*
 * Copyright 2016 Dremio Corporation
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
