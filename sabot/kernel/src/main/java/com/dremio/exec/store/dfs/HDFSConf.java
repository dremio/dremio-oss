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
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.protostuff.Tag;

@SourceType("HDFS")
public class HDFSConf extends FileSystemConf<HDFSConf, FileSystemPlugin> {

  //  optional string hostname = 1;
  //  optional int32 port = 2;
  //  optional bool enableImpersonation = 3 [default = false];
  //  repeated Property property = 4;
  //  optional string root_path = 5 [default = "/"];

  @NotBlank
  @Tag(1)
  public String hostname;

  @Tag(2)
  public int port = 9000;

  @Tag(3)
  public boolean enableImpersonation;

  @JsonProperty("propertyList")
  @Tag(4)
  public List<Property> properties;

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
    return "hdfs://" + hostname + ":" + port + "/";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  @Override
  public FileSystemPlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    return new FileSystemPlugin(this, context, name, null, idProvider);
  }

}
