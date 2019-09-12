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

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.NotBlank;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.protostuff.Tag;

@SourceType(value = "MAPRFS", label = "MapR-FS")
public class MapRConf extends FileSystemConf<MapRConf, FileSystemPlugin<MapRConf>> {

  //  optional string cluster_name = 1;
  //  optional bool enableImpersonation = 2 [default = false];
  //  optional bool secure = 3;
  //  repeated Property property = 4;
  //  optional string root_path = 5 [default = "/"];
  //  optional bool allowCreateDrop = 6;

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Cluster Name")
  public String clusterName;

  @Tag(2)
  @DisplayMetadata(label = "Enable impersonation")
  public boolean enableImpersonation = false;

  @Tag(3)
  @DisplayMetadata(label = "Encrypt connection")
  public boolean secure = false;

  @Tag(4)
  @NotMetadataImpacting
  public List<Property> propertyList = Lists.newArrayList();

  @Tag(5)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  public boolean allowCreateDrop;

  @Override
  public Path getPath() {
    return Path.of(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return enableImpersonation;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  @Override
  public String getConnection() {
    Preconditions.checkNotNull(clusterName);
    return "maprfs://" + clusterName + "/";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return allowCreateDrop ? SchemaMutability.USER_TABLE : SchemaMutability.NONE;
  }

  @Override
  public FileSystemPlugin<MapRConf> newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new FileSystemPlugin<>(this, context, name, pluginIdProvider);
  }

}
