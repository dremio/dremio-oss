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
package com.dremio.dac.cmd.upgrade;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.dremio.common.Version;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.plugins.s3.store.S3PluginConfig;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Before 2.1.7, credential type of S3 source is not set. Set it to NONE if AWS access key is null
 * or empty.
 */
public class UpdateS3CredentialType extends UpgradeTask implements LegacyUpgradeTask {

  // DO NOT MODIFY
  static final String taskUUID = "7512f256-fb80-4517-9a50-55a126fd93d5";

  public UpdateS3CredentialType() {
    super("Update S3 credential type", ImmutableList.of(UpdateDatasetSplitIdTask.taskUUID));
  }

  @Override
  public Version getMaxVersion() {
    return VERSION_217;
  }

  @Override
  public String getTaskUUID() {
    return taskUUID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    final NamespaceService namespaceService =
        new NamespaceServiceImpl(context.getLegacyKVStoreProvider(), new CatalogStatusEventsImpl());

    List<SourceConfig> sources = namespaceService.getSources();

    for (SourceConfig sourceConfig : sources) {
      ConnectionConf<?, ?> connectionConf =
          context.getConnectionReader().getConnectionConf(sourceConfig);
      if (connectionConf instanceof S3PluginConfig) {
        S3PluginConfig s3PluginConfig = (S3PluginConfig) connectionConf;
        if ((s3PluginConfig.credentialType == AWSAuthenticationType.ACCESS_KEY)
            && isNullOrEmpty(s3PluginConfig.accessKey)) {
          s3PluginConfig.credentialType = AWSAuthenticationType.NONE;
          sourceConfig.setConnectionConf(s3PluginConfig);
          namespaceService.addOrUpdateSource(sourceConfig.getKey(), sourceConfig);
        }
      }
    }
  }

  @Override
  public String toString() {
    return String.format("'%s' up to %s)", getDescription(), getMaxVersion());
  }
}
