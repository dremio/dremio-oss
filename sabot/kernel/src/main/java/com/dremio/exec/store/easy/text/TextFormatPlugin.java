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
package com.dremio.exec.store.easy.text;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.physical.base.ScanStats.GroupScanProperty;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.CompleteFileWork;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyGroupScanUtils;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.dfs.easy.ExtendedEasyReaderProperties;
import com.dremio.exec.store.easy.text.compliant.CompliantTextRecordReader;
import com.dremio.exec.store.easy.text.compliant.TextParsingSettings;
import com.dremio.exec.store.text.TextRecordWriter;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class TextFormatPlugin extends EasyFormatPlugin<TextFormatPlugin.TextFormatConfig> {
  private static final String DEFAULT_NAME = "text";

  public TextFormatPlugin(String name, SabotContext context, FileSystemPlugin<?> fsPlugin) {
    super(
        name,
        context,
        new TextFormatConfig(),
        true,
        false,
        true,
        true,
        Collections.<String>emptyList(),
        DEFAULT_NAME,
        fsPlugin);
  }

  public TextFormatPlugin(
      String name,
      SabotContext context,
      TextFormatConfig formatPluginConfig,
      FileSystemPlugin<?> fsPlugin) {
    super(
        name,
        context,
        formatPluginConfig,
        true,
        false,
        true,
        true,
        formatPluginConfig.getExtensions(),
        DEFAULT_NAME,
        fsPlugin);
  }

  @Override
  public RecordReader getRecordReader(
      OperatorContext context,
      FileSystem dfs,
      EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns)
      throws ExecutionSetupException {
    Path path =
        new Path(dfs.makeQualified(com.dremio.io.file.Path.of(splitAttributes.getPath())).toURI());
    FileSplit split =
        new FileSplit(
            path, splitAttributes.getStart(), splitAttributes.getLength(), new String[] {""});
    TextParsingSettings settings = new TextParsingSettings();
    settings.set((TextFormatConfig) formatConfig);
    return new CompliantTextRecordReader(
        split, getFsPlugin().getCompressionCodecFactory(), dfs, context, settings, columns);
  }

  @Override
  public RecordReader getRecordReader(
      OperatorContext context,
      FileSystem dfs,
      EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns,
      ExtendedEasyReaderProperties properties,
      ByteString extendedProperties)
      throws ExecutionSetupException {
    Path path =
        new Path(dfs.makeQualified(com.dremio.io.file.Path.of(splitAttributes.getPath())).toURI());
    FileSplit split =
        new FileSplit(
            path, splitAttributes.getStart(), splitAttributes.getLength(), new String[] {""});
    TextParsingSettings settings = new TextParsingSettings();
    settings.set((TextFormatConfig) formatConfig);
    if (properties.getExtendedFormatOptions() != null
        && properties.getExtendedFormatOptions().getTrimSpace() != null) {
      settings.setIgnoreTrailingWhitespaces(properties.getExtendedFormatOptions().getTrimSpace());
      settings.setIgnoreLeadingWhitespaces(properties.getExtendedFormatOptions().getTrimSpace());
    }
    return new CompliantTextRecordReader(
        split,
        getFsPlugin().getCompressionCodecFactory(),
        dfs,
        context,
        settings,
        columns,
        properties,
        extendedProperties);
  }

  @Override
  public ScanStats getScanStats(final EasyGroupScanUtils scan) {
    long data = 0;
    for (final CompleteFileWork work : scan.getWorkIterable()) {
      data += work.getTotalBytes();
    }
    final double estimatedRowSize =
        getContext().getOptionManager().getOption(ExecConstants.TEXT_ESTIMATED_ROW_SIZE);
    final double estRowCount = data / estimatedRowSize;
    return new ScanStats(
        GroupScanProperty.NO_EXACT_ROW_COUNT, (long) estRowCount, (float) estRowCount, data);
  }

  @Override
  public RecordWriter getRecordWriter(final OperatorContext context, final EasyWriter writer)
      throws IOException {
    final TextFormatConfig textConfig = ((TextFormatConfig) getConfig());
    return new TextRecordWriter(context, writer, textConfig);
  }

  @JsonTypeName("text")
  @JsonInclude(Include.NON_DEFAULT)
  public static class TextFormatConfig implements FormatPluginConfig {

    public List<String> extensions = ImmutableList.of("txt");
    public String lineDelimiter = "\n";
    public String fieldDelimiter = "\u0000";
    public String quote = "\"";
    public String escape = "\"";
    public String comment = "#";
    public boolean skipFirstLine = false;
    public boolean extractHeader = false;
    public boolean autoGenerateColumnNames = false;
    public boolean trimHeader = true;

    public int skipLines = 0;

    /** Extension of files written out with config as part of CTAS. */
    public String outputExtension = "txt";

    public List<String> getExtensions() {
      return extensions;
    }

    public String getQuote() {
      return quote;
    }

    public String getEscape() {
      return escape;
    }

    public String getComment() {
      return comment;
    }

    public String getLineDelimiter() {
      return lineDelimiter;
    }

    public String getFieldDelimiter() {
      return fieldDelimiter;
    }

    @JsonProperty("extractHeader")
    public boolean isHeaderExtractionEnabled() {
      return extractHeader;
    }

    public boolean isAutoGenerateColumnNames() {
      return autoGenerateColumnNames;
    }

    @Deprecated
    @JsonProperty("delimiter")
    public void setFieldDelimiter(String delimiter) {
      this.fieldDelimiter = delimiter;
    }

    public boolean isSkipFirstLine() {
      return skipFirstLine;
    }

    @JsonProperty("trimHeader")
    public boolean isTrimHeaderEnabled() {
      return trimHeader;
    }

    @JsonProperty("skipLines")
    public int getSkipLines() {
      return skipLines;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((comment == null) ? 0 : comment.hashCode());
      result = prime * result + ((escape == null) ? 0 : escape.hashCode());
      result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
      result = prime * result + ((fieldDelimiter == null) ? 0 : fieldDelimiter.hashCode());
      result = prime * result + ((lineDelimiter == null) ? 0 : lineDelimiter.hashCode());
      result = prime * result + ((quote == null) ? 0 : quote.hashCode());
      result = prime * result + (skipFirstLine ? 1231 : 1237);
      result = prime * result + (extractHeader ? 1231 : 1237);
      result = prime * result + (autoGenerateColumnNames ? 1231 : 1237);
      result = prime * result + ((outputExtension == null) ? 0 : outputExtension.hashCode());
      result = prime * result + (trimHeader ? 1231 : 1237);
      result = prime * result + skipLines;
      return result;
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
      TextFormatConfig other = (TextFormatConfig) obj;
      if (!Objects.equals(comment, other.comment)) {
        return false;
      }
      if (!Objects.equals(escape, other.escape)) {
        return false;
      }
      if (extensions == null) {
        if (other.extensions != null) {
          return false;
        }
      } else if (!extensions.equals(other.extensions)) {
        return false;
      }
      if (!Objects.equals(fieldDelimiter, other.fieldDelimiter)) {
        return false;
      }
      if (lineDelimiter == null) {
        if (other.lineDelimiter != null) {
          return false;
        }
      } else if (!lineDelimiter.equals(other.lineDelimiter)) {
        return false;
      }
      if (!Objects.equals(quote, other.quote)) {
        return false;
      }
      if (skipFirstLine != other.skipFirstLine) {
        return false;
      }
      if (extractHeader != other.extractHeader) {
        return false;
      }
      if (autoGenerateColumnNames != other.autoGenerateColumnNames) {
        return false;
      }
      if (trimHeader != other.trimHeader) {
        return false;
      }
      if (skipLines != other.skipLines) {
        return false;
      }
      return Objects.equals(outputExtension, other.outputExtension);
    }
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.TEXT_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    return CoreOperatorType.TEXT_WRITER_VALUE;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public int getMaxFilesLimit() {
    return Math.toIntExact(
        getContext().getOptionManager().getOption(FileDatasetHandle.DFS_MAX_TEXT_FILES));
  }
}
