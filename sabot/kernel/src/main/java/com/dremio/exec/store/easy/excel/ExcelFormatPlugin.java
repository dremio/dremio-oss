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
package com.dremio.exec.store.easy.excel;

import java.io.IOException;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.EasyCoercionReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf.EasyDatasetSplitXAttr;
import com.google.common.collect.Iterables;

/**
 * {@link EasyFormatPlugin} implementation for reading Excel files.
 */
public class ExcelFormatPlugin extends EasyFormatPlugin<ExcelFormatPluginConfig> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelFormatPlugin.class);

  public static final String NAME = "excel";
  private final long maxExcelFileSize;

  public ExcelFormatPlugin(String name, SabotContext context,
      ExcelFormatPluginConfig formatConfig, FileSystemPlugin fsPlugin) {
    super(name, context, formatConfig,
        true, false, /* splittable = */ false, /* compressible = */ false,
        formatConfig.getExtensions(), NAME, fsPlugin);
    maxExcelFileSize = context.getOptionManager().getOption(ExecConstants.EXCEL_MAX_FILE_SIZE_VALIDATOR);
  }

  @Override
  public boolean supportsPushDown() {
    return false;
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(Path.of(splitAttributes.getPath()));
    checkExcelFileSize(path, dfs);
    final ExcelFormatPluginConfig excelFormatConfig = (ExcelFormatPluginConfig) formatConfig;
    return new ExcelRecordReader(
      context,
      dfs,
      path,
      excelFormatConfig,
      columns);
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystem dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns, EasySubScan config) throws ExecutionSetupException {
    RecordReader inner = getRecordReader(context, dfs, splitAttributes, columns);
    return new EasyCoercionReader(context, columns, inner, config.getFullSchema(), Iterables.getFirst(config.getReferencedTables(), null));
  }

  /**
   * Check if the size of the Excel file is lesser than the maximum allowed file size
   */
  private void checkExcelFileSize(final Path path, final FileSystem dfs) {
    try {
      long excelFileSize = dfs.getFileAttributes(path).size();
      if (excelFileSize > maxExcelFileSize) {
        final String errorMessage = String.format("File %s exceeds maximum size limit for Excel files of %d bytes", path, maxExcelFileSize);
        throw UserException
          .unsupportedError()
          .message(errorMessage)
          .build(logger);
      }
    } catch (IOException e) {
      logger.error("Error occurred while fetching file attributes for " + path);
    }
  }

  @Override
  public int getReaderOperatorType() {
    return CoreOperatorType.EXCEL_SUB_SCAN_VALUE;
  }

  @Override
  public RecordWriter getRecordWriter(OperatorContext context, EasyWriter writer) throws IOException {
    throw UserException
        .unsupportedError()
        .message("Writing output in Excel format is not supported")
        .build(logger);
  }

  @Override
  public int getWriterOperatorType() {
    throw UserException
        .unsupportedError()
        .message("Writing output in Excel format is not supported")
        .build(logger);
  }

  @Override
  public int getMaxFilesLimit() {
    return Math.toIntExact(getContext().getOptionManager().getOption(FileDatasetHandle.DFS_MAX_EXCEL_FILES));
  }
}
