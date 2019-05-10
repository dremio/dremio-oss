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
package com.dremio.plugins.elastic;

import java.util.List;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.server.SabotContext;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.mapping.ElasticMappingSet.ElasticMapping;

public class ElasticDatasetHandle implements DatasetHandle {

  private final EntityPath name;
  private final ElasticConnection connection;
  private final SabotContext context;
  private final ElasticsearchConf config;
  private final ElasticMapping mapping;
  private final List<String> aliasIndices;
  private final boolean alias;

  public ElasticDatasetHandle(
    EntityPath name,
    ElasticConnection connection,
    SabotContext context,
    ElasticsearchConf config,
    ElasticMapping mapping,
    List<String> aliasIndices,
    boolean alias
    ) {
    this.name = name;
    this.connection = connection;
    this.context = context;
    this.config = config;
    this.mapping = mapping;
    this.aliasIndices = aliasIndices;
    this.alias = alias;
  }

  ElasticConnection getConnection() {
    return connection;
  }

  SabotContext getContext() {
    return context;
  }

  ElasticsearchConf getPluginConfig() {
    return config;
  }

  ElasticMapping getMapping() {
    return mapping;
  }

  List<String> getAliasIndices() {
    return aliasIndices;
  }

  boolean isAlias() {
    return alias;
  }

  @Override
  public EntityPath getDatasetPath() {
    return name;
  }

}
