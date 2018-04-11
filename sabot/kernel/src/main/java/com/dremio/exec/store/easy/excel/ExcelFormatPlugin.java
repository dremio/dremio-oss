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
package com.dremio.exec.store.easy.excel;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.namespace.file.proto.EasyDatasetSplitXAttr;

/**
 * {@link FormatPlugin} implementation for reading Excel files.
 */
public class ExcelFormatPlugin extends EasyFormatPlugin<ExcelFormatPluginConfig> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelFormatPlugin.class);

  public static final String NAME = "excel";

  public ExcelFormatPlugin(String name, SabotContext context,
      ExcelFormatPluginConfig formatConfig, FileSystemPlugin fsPlugin) {
    super(name, context, formatConfig,
        true, false, /* splittable = */ false, /* compressible = */ false,
        formatConfig.getExtensions(), NAME, fsPlugin);
  }

  @Override
  public boolean supportsPushDown() {
    return false;
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystemWrapper dfs, EasyDatasetSplitXAttr splitAttributes, List<SchemaPath> columns) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(new Path(splitAttributes.getPath()));
    final ExcelFormatPluginConfig excelFormatConfig = (ExcelFormatPluginConfig) formatConfig;
    return new ExcelRecordReader(
        context,
        dfs,
        path,
        excelFormatConfig,
        columns);
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
}
