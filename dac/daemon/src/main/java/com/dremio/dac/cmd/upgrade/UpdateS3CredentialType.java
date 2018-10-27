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

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.plugins.s3.store.S3PluginConfig;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * Before 2.1.7, credential type of S3 source is not set.  Set it to NONE if AWS access key is null or empty.
 */
public class UpdateS3CredentialType extends UpgradeTask {
  public UpdateS3CredentialType() {
    super("Update S3 credential type", VERSION_106, VERSION_217, NORMAL_ORDER);
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());

    List<SourceConfig> sources = namespaceService.getSources();

    for (SourceConfig sourceConfig : sources) {
      ConnectionConf<?, ?> connectionConf = context.getConnectionReader().getConnectionConf(sourceConfig);
      if (connectionConf instanceof S3PluginConfig) {
        S3PluginConfig s3PluginConfig = (S3PluginConfig) connectionConf;
        if ((s3PluginConfig.credentialType == S3PluginConfig.AuthenticationType.ACCESS_KEY) && isNullOrEmpty(s3PluginConfig.accessKey)) {
          s3PluginConfig.credentialType = S3PluginConfig.AuthenticationType.NONE;
          sourceConfig.setConnectionConf(s3PluginConfig);
          namespaceService.addOrUpdateSource(sourceConfig.getKey(), sourceConfig);
        }
      }
    }
  }
}
