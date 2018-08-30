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
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;

import io.protostuff.Tag;

@SourceType("HDFS")
public class HDFSConf extends FileSystemConf<HDFSConf, FileSystemPlugin> {
  public enum ShortCircuitFlag {
    @Tag(1) @DisplayMetadata(label = "HDFS Default") SYSTEM,
    @Tag(2) @DisplayMetadata(label = "Enabled") ENABLED,
    @Tag(3) @DisplayMetadata(label = "Disabled") DISABLED;
  }

  //  optional string hostname = 1;
  //  optional int32 port = 2;
  //  optional bool enable_impersonation = 3 [default = false];
  //  repeated Property property = 4;
  //  optional string root_path = 5 [default = "/"];
  //  optional ShortCircuitFlag short_circuit_enabled = 6
  //  optional string short_circuit_socket_path = 7

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "NameNode Host")
  public String hostname;

  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 8020;

  @Tag(3)
  @DisplayMetadata(label = "Enable impersonation")
  public boolean enableImpersonation;

  @Tag(4)
  public List<Property> propertyList;

  @Tag(5)
  @DisplayMetadata(label = "Root Path")
  public String rootPath = "/";

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Short-Circuit Local Reads")
  public ShortCircuitFlag shortCircuitFlag = ShortCircuitFlag.SYSTEM;

  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Socket Path")
  public String shortCircuitSocketPath;

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
    return propertyList;
  }

  @Override
  public String getConnection() {
    return "hdfs://" + hostname + ":" + port + "/";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.NONE;
  }

  public ShortCircuitFlag getShortCircuitFlag() {
    return shortCircuitFlag;
  }

  public String getShortCircuitSocketPath() {
    return shortCircuitSocketPath;
  }

  @Override
  public FileSystemPlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    return new FileSystemPlugin(this, context, name, null, idProvider);
  }

}
