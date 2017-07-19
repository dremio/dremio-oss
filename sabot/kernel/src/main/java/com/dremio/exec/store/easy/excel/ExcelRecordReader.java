/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.exec.store.easy.excel;

import java.io.InputStream;
import java.util.List;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.Path;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.easy.excel.ExcelParser.State;
import com.dremio.exec.store.easy.excel.xls.XlsInputStream;
import com.dremio.exec.store.easy.excel.xls.XlsRecordProcessor;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;

/**
 * {@link RecordReader} implementation for reading a single sheet in an Excel file. Current support is
 * only for XLSX types (XML based format). Support for XLS (binary format) is not yet available.
 */
public class ExcelRecordReader extends AbstractRecordReader implements XlsInputStream.BufferManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelRecordReader.class);

  private final OperatorContext executionContext;
  private final FileSystemWrapper dfs;
  private final Path path;
  private final ExcelFormatPluginConfig pluginConfig;

  private VectorContainerWriter writer;

  private ExcelParser parser;
  private InputStream inputStream;

  private long runningRecordCount = 0;

  public ExcelRecordReader(final OperatorContext executionContext, final FileSystemWrapper dfs, final Path path,
      final ExcelFormatPluginConfig pluginConfig, final List<SchemaPath> columns) {
    super(executionContext, columns);
    this.executionContext = executionContext;
    this.dfs = dfs;
    this.path = path;
    this.pluginConfig = pluginConfig;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    // Reason for enabling the union by default is because Excel documents are highly likely to contain
    // mixed types and the sampling code doesn't take enable union by default.
    this.writer = new VectorContainerWriter(output);
    final ArrowBuf managedBuf = executionContext.getManagedBuffer();

    try {
      inputStream = dfs.open(path);

      if (pluginConfig.xls) {
        // we don't need to close this stream, it will be closed by the parser
        final XlsInputStream xlsStream = new XlsInputStream(this, inputStream);
        parser = new XlsRecordProcessor(xlsStream, pluginConfig, writer, managedBuf);
      } else {
        parser = new StAXBasedParser(inputStream, pluginConfig, writer, managedBuf);
      }
    } catch (final SheetNotFoundException e) {
      // This check will move to schema validation in planning after DX-2271
      throw UserException.validationError()
              .message("There is no sheet with given name '%s' in Excel document located at '%s'",
                      pluginConfig.sheet, path)
              .build(logger);
    } catch (Throwable e) {
      throw UserException.dataReadError(e)
              .message("Failure creating parser for entry '%s'", path)
              .build(logger);
    }
  }

  @Override
  public ArrowBuf allocate(int size) {
    return executionContext.getManagedBuffer(size);
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();

    int recordCount = 0;
    try {
      while (recordCount < numRowsPerBatch) {
        writer.setPosition(recordCount);

        final ExcelParser.State state = parser.parseNextRecord();

        if (state == State.READ_SUCCESSFUL) {
          recordCount++;
          runningRecordCount++;
        } else {
          break;
        }
      }

      writer.setValueCount(recordCount);
      return recordCount;
    } catch (final Exception e) {
      throw UserException.dataReadError(e)
          .message("Failed to parse records in row number %d", runningRecordCount)
          .build(logger);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(parser, inputStream);
  }
}
