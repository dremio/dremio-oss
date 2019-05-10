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
package com.dremio.plugins.azure;

import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

/**
 * Azure Storage (including datalake v2)
 */
@SourceType(value = "AZURE_STORAGE", label = "Azure Storage")
public class AzureStorageConf extends FileSystemConf<AzureStorageConf, AzureStoragePlugin> {

  private static final List<String> UNIQUE_CONN_PROPS = ImmutableList.of(
      AzureStorageFileSystem.ACCOUNT,
      AzureStorageFileSystem.SECURE,
      AzureStorageFileSystem.KEY,
      AzureStorageFileSystem.CONTAINER_LIST
  );


  /**
   * Type of Storage
   */
  public enum AccountKind {
    @Tag(1)
    @DisplayMetadata(label = "StorageV1")
    STORAGE_V1,

    @Tag(2)
    @DisplayMetadata(label = "StorageV2")
    STORAGE_V2
    ;

    @JsonIgnore
    public Prototype getPrototype(boolean enableSSL) {
      if(this == AccountKind.STORAGE_V1) {
        return enableSSL ? Prototype.WASBS : Prototype.WASB;
      } else {
        return enableSSL ? Prototype.ABFSS: Prototype.ABFS;
      }
    }
  }

  @Tag(1)
  @DisplayMetadata(label = "Account Kind")
  public AccountKind accountKind = AccountKind.STORAGE_V2;

  @Tag(2)
  @DisplayMetadata(label = "Account Name")
  public String accountName;

  @Tag(3)
  @Secret
  @DisplayMetadata(label = "Shared Access Key")
  public String accessKey;

  @Tag(4)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(5)
  @DisplayMetadata(label = "Advanced Properties")
  public List<Property> propertyList;

  @Tag(6)
  @DisplayMetadata(label = "Blob Containers & Filesystem Whitelist")
  public List<String> containers;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Encrypt connection")
  public boolean enableSSL = true;

  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  public boolean allowCreateDrop = false;

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable asynchronous access when possible")
  public boolean enableAsync = true;

  @Override
  public AzureStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    Preconditions.checkNotNull(accountName, "Account name must be provided.");
    return new AzureStoragePlugin(this, context, name, pluginIdProvider);
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
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    return String.format("%s:///", AzureStorageFileSystem.SCHEME);
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
  public boolean isAsyncEnabled() {
    return enableAsync;
  }
}
