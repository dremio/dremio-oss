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
package com.dremio.exec.store.easy.json;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EasyCoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.dfs.easy.ExtendedEasyReaderProperties;
import com.dremio.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig> {

  private static final boolean IS_COMPRESSIBLE = true;
  private static final String DEFAULT_NAME = "json";

  public JSONFormatPlugin(String name, SabotContext context, FileSystemPlugin<?> fsPlugin) {
    this(name, context, new JSONFormatConfig(), fsPlugin);
  }

  public JSONFormatPlugin(String name, SabotContext context, JSONFormatConfig formatPluginConfig, FileSystemPlugin<?> fsPlugin) {
    super(name, context, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, formatPluginConfig.getExtensions(), DEFAULT_NAME, fsPlugin);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns) throws ExecutionSetupException {
    return new JSONRecordReader(context, splitAttributes.getPath(), getFsPlugin().getCompressionCodecFactory(), dfs, columns);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns, ExtendedEasyReaderProperties properties) throws ExecutionSetupException {
    return new JSONRecordReader(context, splitAttributes.getPath(), getFsPlugin().getCompressionCodecFactory(), dfs, columns, properties.isSchemaImposed(), properties.getExtendedFormatOptions());
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns, EasySubScan config) throws ExecutionSetupException {
    RecordReader inner = getRecordReader(context, dfs, splitAttributes, columns);
    return new EasyCoercionReader(context, columns, inner, config.getFullSchema(), Iterables.getFirst(config.getReferencedTables(), null), config.getUserDefinedSchemaSettings());
  }

  @Override
  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException {
    RecordWriter recordWriter = new JsonRecordWriter(context, writer, (JSONFormatConfig) getConfig());
    return recordWriter;
  }

  @JsonTypeName("json")
  public static class JSONFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of("json");
    private static final List<String> DEFAULT_EXTS = ImmutableList.of("json");

    /**
     * Extension of files written out with config as part of CTAS.
     */
    public String outputExtension = "json";

    public boolean prettyPrint = true;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        // when loading an old JSONFormatConfig that doesn't contain an "extensions" attribute
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    public boolean isPrettyPrint() {
      return prettyPrint;
    }

    public void setPrettyPrint(boolean prettyPrint) {
      this.prettyPrint = prettyPrint;
    }

    @Override
    public int hashCode() {
      return Objects.hash(extensions, outputExtension, prettyPrint);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      JSONFormatConfig other = (JSONFormatConfig) obj;

      return Objects.equals(extensions, other.extensions) &&
          Objects.equals(outputExtension, other.outputExtension) &&
          Objects.equals(prettyPrint, other.prettyPrint);
    }
  }

  @Override
  public FormatMatcher getMatcher() {
    return super.getMatcher();
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.JSON_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

}
