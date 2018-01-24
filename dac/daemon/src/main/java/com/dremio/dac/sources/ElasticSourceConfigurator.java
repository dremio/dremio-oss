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
package com.dremio.dac.sources;

import static com.dremio.dac.sources.Hosts.getHosts;
import static com.dremio.service.namespace.source.proto.SourceType.ELASTIC;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.AuthenticationType;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.plugins.elastic.ElasticsearchStoragePluginConfig;
import com.google.common.base.Preconditions;

/**
 * generates a StoragePluginConfig from an Elastic Source
 */
public class ElasticSourceConfigurator extends SingleSourceToStoragePluginConfig<ElasticConfig> {

  public ElasticSourceConfigurator() {
    super(ELASTIC);
  }

  @Override
  public StoragePluginConfig configureSingle(ElasticConfig elastic) {
    final String username, password;
    if (elastic.getAuthenticationType() == AuthenticationType.ANONYMOUS) {
      username = null;
      password = null;
    } else {
      username = elastic.getUsername();
      password = elastic.getPassword();
    }

    Preconditions.checkArgument(elastic.getScrollTimeoutMillis() > 0, "Scroll timeout must be greater than 0.");
    Preconditions.checkArgument(elastic.getReadTimeoutMillis() > 0, "Read timeout must be greater than 0.");

    ElasticsearchStoragePluginConfig config = new ElasticsearchStoragePluginConfig(
      getHosts(checkNotNull(elastic.getHostList(), "missing hostList"), ','),
      elastic.getScrollSize(),
      elastic.getScrollTimeoutMillis(),
      elastic.getReadTimeoutMillis(),
      getBooleanElseFalse(elastic.getShowHiddenIndices()),
      getBooleanElseFalse(elastic.getScriptsEnabled()),
      getBooleanElseTrue(elastic.getUsePainless()),
      getBooleanElseFalse(elastic.getShowIdColumn()),
      username,
      password,
      getBooleanElseFalse(elastic.getSslEnabled()),
      getBooleanElseFalse(elastic.getUseWhitelist()),
      getBooleanElseFalse(elastic.getAllowGroupByOnNormalizedFields()));
    return config;
  }

  private boolean getBooleanElseFalse(Boolean value) {
    return value == null ? false : value.booleanValue();
  }

  private boolean getBooleanElseTrue(Boolean value) {
    return value == null ? true : value.booleanValue();
  }
}
