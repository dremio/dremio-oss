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
package com.dremio.exec.store.dfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import com.dremio.common.FSConstants;
import com.dremio.common.util.S3ConnectionConstants;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.google.common.base.Strings;

/**
 * Storage plugin for internal filesystems that may be configured with dist path.
 */
public class MayBeDistFileSystemPlugin<C extends MayBeDistFileSystemConf<C, ?>> extends FileSystemPlugin<C> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MayBeDistFileSystemPlugin.class);

  public MayBeDistFileSystemPlugin(C config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  protected List<Property> getProperties() {
    List<Property> props = new ArrayList<>(super.getProperties());

    // copy over only if keys are used for data credentials
    // roles will be available as instance roles.
    if (!Strings.isNullOrEmpty(getConfig().getSecretKey())) {
      // both go together and data credentials already validates that both are present
      props.add(new Property(FSConstants.FS_S3A_ACCESS_KEY, getConfig().getAccessKey()));
      props.add(new Property(FSConstants.FS_S3A_SECRET_KEY, getConfig().getSecretKey()));
    } else if (!Strings.isNullOrEmpty(getConfig().getClientSecret())) {
      props.add(new Property(FSConstants.AZURE_ACCOUNT, getConfig().getAccountName()));
      props.add(new Property(FSConstants.AZURE_SECURE, Boolean.toString(Boolean.TRUE)));
      props.add(new Property(FSConstants.AZURE_MODE, getConfig().getAccountKind()));
      props.add(new Property(FSConstants.AZURE_CLIENT_ID, getConfig().getClientId()));
      props.add(new Property(FSConstants.AZURE_CLIENT_SECRET, getConfig().getClientSecret()));
      props.add(new Property(FSConstants.AZURE_TOKEN_ENDPOINT, getConfig().getTokenEndpoint()));
    }
    props.add(new Property(FSConstants.MAXIMUM_CONNECTIONS, String.valueOf(S3ConnectionConstants.DEFAULT_MAX_CONNECTIONS)));
    props.add(new Property(FSConstants.MAX_THREADS, String.valueOf(S3ConnectionConstants.DEFAULT_MAX_THREADS)));
    props.add(new Property("fs.dremioS3.impl.disable.cache","true"));
    props.add(new Property("fs.dremiogcs.impl.disable.cache","true"));
    props.add(new Property("fs.dremioAzureStorage.impl.disable.cache", "true"));
    props.add(new Property("fs.dremioAdl.impl.disable.cache", "true"));
    return props;
  }

  @Override
  protected FileSystem newFileSystem(String userName, OperatorContext operatorContext) throws IOException {
    if (!Strings.isNullOrEmpty(getConfig().getSecretKey())) {
      getFsConf().set("fs.dremioS3.impl", "com.dremio.plugins.s3.store.S3FileSystem");
      getFsConf().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    } else if (!Strings.isNullOrEmpty(getConfig().getIamRole())) {
      getFsConf().set("fs.dremioS3.impl", "com.dremio.plugins.s3.store.S3FileSystem");
      // in executor we should use the associated instance profile
      if (!getContext().getRoles().contains(ClusterCoordinator.Role.EXECUTOR)) {
        getFsConf().set("fs.s3a.aws.credentials.provider", "com.dremio.service.coordinator" +
          ".DremioAssumeRoleCredentialsProviderV1");
      }
    } else if (!Strings.isNullOrEmpty(getConfig().getClientSecret())) {
      getFsConf().set("fs.dremioAzureStorage.impl", "com.dremio.plugins.azure.AzureStorageFileSystem");
      getFsConf().set("dremio.azure.credentialsType", "AZURE_ACTIVE_DIRECTORY");
    }
    return super.newFileSystem(userName, operatorContext);
  }

}
