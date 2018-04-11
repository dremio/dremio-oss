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
package com.dremio.exec.store.dfs;

import java.util.List;

import javax.inject.Provider;

import org.apache.hadoop.fs.Path;
import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.protostuff.Tag;

@SourceType("MAPRFS")
public class MapRConf extends FileSystemConf<MapRConf, FileSystemPlugin> {

  //  optional string cluster_name = 1;
  //  optional bool enableImpersonation = 2 [default = false];
  //  optional bool secure = 3;
  //  repeated Property property = 4;
  //  optional string root_path = 5 [default = "/"];

  @NotBlank
  @Tag(1)
  public String clusterName;

  @Tag(2)
  public boolean enableImpersonation = false;

  @Tag(3)
  public boolean secure = false;

  @JsonProperty("propertyList")
  @Tag(4)
  @NotMetadataImpacting
  public List<Property> properties = Lists.newArrayList();

  @Tag(5)
  public String rootPath = "/";

  @Override
  public Path getPath() {
    return new Path(rootPath);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return enableImpersonation;
  }

  @Override
  public List<Property> getProperties() {
    return properties;
  }

  @Override
  public String getConnection() {
    Preconditions.checkNotNull(clusterName);
    return "maprfs://" + clusterName + "/";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public FileSystemPlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new FileSystemPlugin(this, context, name, null, pluginIdProvider);
  }

}
