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
package com.dremio.plugins.s3.store;

import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Connection Configuration for S3.
 */
@SourceType(value = "S3", label = "Amazon S3")
public class S3PluginConfig extends FileSystemConf<S3PluginConfig, S3StoragePlugin> {

  static final List<String> UNIQUE_CONN_PROPS = ImmutableList.of(
    Constants.ACCESS_KEY,
    Constants.SECRET_KEY,
    Constants.SESSION_TOKEN,
    Constants.SECURE_CONNECTIONS,
    Constants.ENDPOINT,
    Constants.AWS_CREDENTIALS_PROVIDER,
    Constants.MAXIMUM_CONNECTIONS,
    Constants.MAX_ERROR_RETRIES,
    Constants.ESTABLISH_TIMEOUT,
    Constants.SOCKET_TIMEOUT,
    Constants.SOCKET_SEND_BUFFER,
    Constants.SOCKET_RECV_BUFFER,
    Constants.SIGNING_ALGORITHM,
    Constants.USER_AGENT_PREFIX,
    Constants.PROXY_HOST,
    Constants.PROXY_PORT,
    Constants.PROXY_DOMAIN,
    Constants.PROXY_USERNAME,
    Constants.PROXY_PASSWORD,
    Constants.PROXY_WORKSTATION,
    Constants.PATH_STYLE_ACCESS,
    S3FileSystem.COMPATIBILITY_MODE,
    S3FileSystem.REGION_OVERRIDE
  );

  @Tag(1)
  @DisplayMetadata(label = "AWS Access Key")
  public String accessKey = "";

  @Tag(2)
  @Secret
  @DisplayMetadata(label = "AWS Access Secret")
  public String accessSecret = "";
  
  @Tag(3)
  @DisplayMetadata(label = "AWS  Session Token")
  public String accessToken = "";

  @Tag(4)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Encrypt connection")
  public boolean secure;

  @Tag(5)
  @DisplayMetadata(label = "External Buckets")
  public List<String> externalBucketList;

  @Tag(6)
  public List<Property> propertyList;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  public boolean allowCreateDrop;

  @Tag(8)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(9)
  public AWSAuthenticationType credentialType = AWSAuthenticationType.ACCESS_KEY;

  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean enableAsync = true;

  @Tag(11)
  @DisplayMetadata(label = "Enable compatibility mode (experimental)")
  public boolean compatibilityMode = false;

 

  @Override
  public S3StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new S3StoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public Path getPath() {
    return new Path(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    return "dremioS3:///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return allowCreateDrop ? SchemaMutability.USER_TABLE : SchemaMutability.NONE;
  }

  @Override
  public List<String> getConnectionUniqueProperties() {
    return UNIQUE_CONN_PROPS;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public boolean isAsyncEnabled() {
    return enableAsync;
  }
}
