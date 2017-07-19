/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.exec.store.easy.excel;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * {@link FormatPlugin} implementation for reading Excel files.
 */
public class ExcelFormatPlugin extends EasyFormatPlugin<ExcelFormatPluginConfig> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelFormatPlugin.class);

  public static final String NAME = "excel";

  public ExcelFormatPlugin(String name, SabotContext context,
      StoragePluginConfig storageConfig, ExcelFormatPluginConfig formatConfig, FileSystemPlugin fsPlugin) {
    super(name, context, storageConfig, formatConfig,
        true, false, /* splittable = */ false, /* compressible = */ false,
        formatConfig.getExtensions(), NAME, fsPlugin);
  }

  @Override
  public boolean supportsPushDown() {
    return false;
  }

  @Override
  public RecordReader getRecordReader(OperatorContext context, FileSystemWrapper dfs, FileWork fileWork,
      List<SchemaPath> columns) throws ExecutionSetupException {
    final Path path = dfs.makeQualified(new Path(fileWork.getPath()));
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
