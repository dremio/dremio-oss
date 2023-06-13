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
import com.dremio.exec.catalog.conf.DefaultCtasFormatSelection;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.io.file.Path;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;

import io.protostuff.Tag;

@SourceType(value = "NAS", uiConfig = "nas-layout.json")
public class NASConf extends FileSystemConf<NASConf, FileSystemPlugin<NASConf>> {

  // optional string path = 1;

  @NotBlank
  @Tag(1)
  @DisplayMetadata(label = "Mount Path")
  public String path;

  @Tag(2)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable exports into the source (CTAS and DROP)")
  @JsonIgnore
  public boolean allowCreateDrop;

  @Tag(3)
  public List<Property> propertyList;


  @Tag(4)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default CTAS Format")
  public DefaultCtasFormatSelection defaultCtasFormat = DefaultCtasFormatSelection.ICEBERG;

  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable partition column inference")
  public boolean isPartitionInferenceEnabled = false;

  @Override
  public Path getPath() {
    return Path.of(path);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList == null ? ImmutableList.of() : propertyList;
  }

  @Override
  public String getConnection() {
    return "file:///";
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return isPartitionInferenceEnabled;
  }

  @Override
  public FileSystemPlugin<NASConf> newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
    return new NASFileSystem(this, context, name, pluginIdProvider);
  }

  @Override
  public String getDefaultCtasFormat() {
    return defaultCtasFormat.getDefaultCtasFormat();
  }
}
