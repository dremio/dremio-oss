/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.server;

import static com.dremio.service.namespace.source.proto.SourceType.NAS;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.exec.store.avro.AvroFormatConfig;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.exec.store.easy.json.JSONFormatPlugin;
import com.dremio.exec.store.easy.sequencefile.SequenceFileFormatConfig;
import com.dremio.exec.store.easy.text.TextFormatPlugin;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.google.common.collect.Lists;

/**
 * generates a StoragePluginConfig for NAS Sources
 *
 */
public class NASSourceConfigurator extends SingleSourceToStoragePluginConfig<NASConfig> {

  public NASSourceConfigurator() {
    super(NAS);
  }

  /**
   * Creates a {@link TextFormatPlugin.TextFormatConfig}.
   *
   * Dremio populates the default values from a config file read by Jackson,
   * so the TextFormatConfig class doesn't have a useful constructor.
   *
   * @return - a new TextFormatConfig
   */
  public static TextFormatPlugin.TextFormatConfig createTextFormatPlugin(boolean extractHeader,
      char fieldDelimiter,
      List<String> extensions) {
    TextFormatPlugin.TextFormatConfig newText = new TextFormatPlugin.TextFormatConfig();
    newText.extractHeader = extractHeader;
    newText.fieldDelimiter = fieldDelimiter;
    newText.extensions = extensions;
    // Use the default values for all other fields for now
    return newText;
  }

  public static Map<String, FormatPluginConfig> getDefaultFormats() {
    Map<String, FormatPluginConfig> defaultFormats = new TreeMap<>();
    defaultFormats.put("csv", createTextFormatPlugin(false, ',', Lists.newArrayList("csv")));
    defaultFormats.put("csvh", createTextFormatPlugin(true, ',', Lists.newArrayList("csvh")));
    defaultFormats.put("tsv", createTextFormatPlugin(false, '\t', Lists.newArrayList("tsv")));
    defaultFormats.put("psv", createTextFormatPlugin(false, '|', Lists.newArrayList("psv", "tbl")));
    defaultFormats.put("parquet", new ParquetFormatConfig());
    defaultFormats.put("avro", new AvroFormatConfig());
    defaultFormats.put("json", new JSONFormatPlugin.JSONFormatConfig());
    SequenceFileFormatConfig seq = new SequenceFileFormatConfig();
    seq.extensions = Lists.newArrayList("seq");
    defaultFormats.put("sequencefile", seq);
    return defaultFormats;
  }

  @Override
  public StoragePluginConfig configureSingle(NASConfig nas) {
    String path = checkNotNull(nas.getPath(), "missing path");
    String connection = "file:///";
    FileSystemConfig config = new FileSystemConfig(connection, path, null, getDefaultFormats(),
        /*impersonationEnabled=*/false /* impersonation is not supported in NAS */, SchemaMutability.NONE);
    return config;
  }

}
