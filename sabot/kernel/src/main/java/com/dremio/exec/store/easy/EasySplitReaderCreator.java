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
package com.dremio.exec.store.easy;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.ExtendedFormatOptions;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.ExtendedEasyReaderProperties;
import com.dremio.exec.store.parquet.InputStreamProvider;
import com.dremio.exec.store.parquet.MutableParquetMetadata;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import io.protostuff.ByteString;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * A lightweight object used to manage the creation of a reader. Allows pre-initialization of data
 * before reader construction.
 */
public class EasySplitReaderCreator extends SplitReaderCreator implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(EasySplitReaderCreator.class);
  private SplitAndPartitionInfo datasetSplit;
  private FileSystem fs;
  protected EasyProtobuf.EasyDatasetSplitXAttr easySplitXAttr;
  private final OperatorContext context;
  private final List<SchemaPath> columns;
  private final EasyFormatPlugin formatPlugin;
  private final CompressionCodecFactory compressionCodecFactory;
  private final ExtendedFormatOptions extendedFormatOptions;
  private final ByteString extendedProperty;

  public EasySplitReaderCreator(
      OperatorContext context,
      FileSystem fs,
      SplitAndPartitionInfo splitInfo,
      List<List<String>> tablePath,
      List<SchemaPath> columns,
      FormatPlugin formatPlugin,
      CompressionCodecFactory compressionCodecFactory,
      ExtendedFormatOptions extendedFormatOptions,
      ByteString extendedProperty) {
    this.datasetSplit = splitInfo;

    try {
      this.easySplitXAttr =
          EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(
              datasetSplit.getDatasetSplitInfo().getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
    this.path = Path.of(easySplitXAttr.getPath());
    this.tablePath = tablePath;
    if (!fs.supportsPath(path)) {
      throw UserException.invalidMetadataError()
          .addContext("%s: Invalid FS for file '%s'", fs.getScheme(), path)
          .setAdditionalExceptionContext(
              new InvalidMetadataErrorContext(ImmutableList.copyOf(tablePath)))
          .buildSilently();
    }
    this.context = context;
    this.fs = fs;
    this.columns = columns;
    this.formatPlugin = (EasyFormatPlugin) formatPlugin;
    this.compressionCodecFactory = compressionCodecFactory;
    this.extendedFormatOptions = extendedFormatOptions;
    this.extendedProperty = extendedProperty;
  }

  @Override
  public void addRowGroupsToRead(Set<Integer> rowGroupsToRead) {}

  @Override
  public SplitAndPartitionInfo getSplit() {
    return this.datasetSplit;
  }

  @Override
  public void createInputStreamProvider(
      InputStreamProvider lastInputStreamProvider, MutableParquetMetadata lastFooter) {}

  @Override
  protected <T> T handleEx(RunnableIO<T> r) {
    Preconditions.checkNotNull(easySplitXAttr);
    try {
      return r.run();
    } catch (FileNotFoundException e) {
      throw UserException.invalidMetadataError(e)
          .addContext("File not found")
          .addContext("File", easySplitXAttr.getPath())
          .build(logger);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .addContext("Failure opening file")
          .addContext("File", easySplitXAttr.getPath())
          .build(logger);
    }
  }

  @Override
  public RecordReader createRecordReader(MutableParquetMetadata footer) {
    return handleEx(
        () -> {
          try {
            RecordReader inner = getEasyRecordReader();
            return inner;
          } finally {
            this.inputStreamProvider = null;
          }
        });
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(inputStreamProvider);
    inputStreamProvider = null;
  }

  private RecordReader getEasyRecordReader() {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("'COPY INTO' Getting record reader for {}", easySplitXAttr.toString());
      }
      return formatPlugin.getRecordReader(
          context,
          fs,
          easySplitXAttr,
          columns,
          getExtendedEasyReaderProperties(),
          extendedProperty);
    } catch (ExecutionSetupException e) {
      throw UserException.dataReadError(e).message("Unable to get record reader").buildSilently();
    }
  }

  private ExtendedEasyReaderProperties getExtendedEasyReaderProperties() {
    return new ExtendedEasyReaderProperties.Builder(true, extendedFormatOptions).build();
  }
}
