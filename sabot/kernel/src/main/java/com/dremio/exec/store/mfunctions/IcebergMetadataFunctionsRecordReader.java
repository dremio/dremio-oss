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

package com.dremio.exec.store.mfunctions;

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_METADATA_FUNCTIONS;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;

/**
 * Record reader for tables related to iceberg metadata functions.
 */
final class IcebergMetadataFunctionsRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IcebergMetadataFunctionsRecordReader.class);

  private final String metadataLocation;
  private final List<String> dataset;
  private final OperatorContext context;
  private final SupportsIcebergRootPointer pluginForIceberg;
  private final OpProps props;
  private ArrowBuf tmpBuf;
  private IcebergMetadataValueVectorWriter valueWriter;
  private final List<SchemaPath> columns;
  private final Table icebergTable ;

  public IcebergMetadataFunctionsRecordReader(OperatorContext context,
                                    SupportsIcebergRootPointer pluginForIceberg,
                                    MetadataFunctionsSubScan config) {
    if (context.getOptions() != null && !context.getOptions().getOption(ENABLE_ICEBERG_METADATA_FUNCTIONS)) {
      throw UserException.unsupportedError().message("Query on metadata functions is not supported on iceberg.").buildSilently();
    }
    Preconditions.checkNotNull(config.getReferencedTables());
    this.metadataLocation = config.getMetadataLocation();
    this.context = context;
    this.pluginForIceberg = pluginForIceberg;
    this.dataset = config.getReferencedTables().iterator().next();
    this.props = config.getProps();
    this.columns = config.getColumns();
    MetadataTableType tableType = IcebergMetadataFunctionsTable.valueOf(config.getmFunction().getName().toUpperCase(Locale.ROOT)).getTableType();
    this.icebergTable = MetadataTableUtils.createMetadataTableInstance(getTableOps(),null ,null, tableType);
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.tmpBuf = context.getAllocator().buffer(4096);
    this.valueWriter = new IcebergMetadataValueVectorWriter(output, context.getTargetBatchSize(),
      columns, icebergTable.schema(), icebergTable
      .newScan()
      .planFiles()
      .iterator(),tmpBuf);
  }


  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }


  @Override
  public int next() {
    Preconditions.checkNotNull(valueWriter, "Writer must be #setup first");
    return this.valueWriter.write();
  }


  @Override
  public void close() throws Exception {
    context.getStats().setReadIOStats();
    AutoCloseables.close(tmpBuf);
  }

  private TableOperations getTableOps() {
    FileSystem fs;
    try {
      fs = pluginForIceberg.createFSWithAsyncOptions(metadataLocation, props.getUserName(), context);
    } catch (IOException e) {
      throw new RuntimeException("Failed creating filesystem", e);
    }
    return new StaticTableOperations(metadataLocation, new DremioFileIO(
      fs, context, dataset, null, null, pluginForIceberg.getFsConfCopy(), (MutablePlugin)pluginForIceberg));
  }

}
