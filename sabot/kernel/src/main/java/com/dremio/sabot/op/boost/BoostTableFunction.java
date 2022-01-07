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
package com.dremio.sabot.op.boost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.BoostTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.exec.store.parquet.ParquetSplitReaderCreatorIterator;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.io.file.BoostedFileSystem;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.google.common.base.Preconditions;

public class BoostTableFunction extends ParquetScanTableFunction {

  private static final Logger logger = LoggerFactory.getLogger(BoostTableFunction.class);
  private List<String> columnsToBoost = new ArrayList();
  private BoostedFileSystem boostedFS;
  private final List<ArrowColumnWriter> currentWriters;
  private List<String> dataset = functionConfig.getFunctionContext().getReferencedTables().iterator().next();
  private RecordReaderIterator recordReaderIterator;
  private VectorContainer boostOutputContainer;
  private VarCharVector columnVector;

  // specifies schema of BoostOperator output
  static String COLUMN = "Column";

  public static BatchSchema SCHEMA = BatchSchema.newBuilder()
    .addField(new Field(COLUMN, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList()))
    .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
    .build();

  Field COLUMN_FIELD = SCHEMA.getColumn(0);

  public BoostTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) throws ExecutionSetupException {
    super(fec, context, props, functionConfig);
    this.fec = fec;
    this.props = props;

    for (SchemaPath column : functionConfig.getFunctionContext().getColumns()) {
      if (column.isSimplePath()) {
        columnsToBoost.add(column.getRootSegment().getPath());
      } else {
        logger.error("Skipping complex column {}", column);
      }
    }
    currentWriters = new ArrayList<>();
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    FileSystem fs ;
    FileSystemPlugin<?> plugin = fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());

    try {
      fs = plugin.createFS(props.getUserName(), context);
    } catch (IOException e) {
      throw new ExecutionSetupException("Unable to setup filesystem for boosting.", e);
    }

    if (!fs.supportsBoosting()) {
      throw new RuntimeException("Provided Filesystem does not support boosting");
    }
    boostedFS = fs.getBoostedFilesystem();
    boostOutputContainer = context.createOutputVectorContainer();
    columnVector = boostOutputContainer.addOrGet(COLUMN_FIELD);
    this.outgoing = context.createOutputVectorContainer();
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    super.startRow(row);
    setUpNewWriters();
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    int rowsProcessed = super.processRow(startOutIndex, maxRecords);
    List<ArrowColumnWriter> failedWriters = new ArrayList<>();
    for (ArrowColumnWriter writer : currentWriters) {
      try {
        writer.write(rowsProcessed);
      } catch (IOException ex) {
        logger.error("Failed to write record batch of column [{}] to boost file, skipping column", writer.column, ex);
        writer.abort();
        failedWriters.add(writer);
      }
    }
    return rowsProcessed;
  }

  @Override
  public void closeRow() throws Exception {
    super.closeRow();
    closeCurrentWritersAndProduceOutputBatch();
  }

  @Override
  public void close() throws Exception {
    BoostTableFunctionContext boostTableFunctionContext = (BoostTableFunctionContext) functionConfig.getFunctionContext();
    context.getSpillService().deleteSpillSubdirs(boostTableFunctionContext.getSplitDir());
    super.close();
  }

  private void setUpNewWriters() {
    ParquetProtobuf.ParquetDatasetSplitScanXAttr splitXAttr = getRecordReaderIterator().getCurrentSplitXAttr();
    Preconditions.checkArgument(splitXAttr != null, "splitXAttr can not be null");
    for (String columnName : columnsToBoost) {
      ArrowColumnWriter arrowColumnWriter;
      try {
        arrowColumnWriter = new ArrowColumnWriter(splitXAttr, columnName, context, boostedFS, dataset);
        arrowColumnWriter.setup(fieldVectorMap.get(columnName.toLowerCase()));
      } catch (IOException ex) {
        logger.debug("Failed to initialize column writer to boost column [{}] of split [{}] due to {}. Skipping column.", columnName, splitXAttr.getPath(), ex.getMessage());
        continue;
      }
      currentWriters.add(arrowColumnWriter);
    }
  }

  private int closeCurrentWritersAndProduceOutputBatch() {
    List<ArrowColumnWriter> failedWriters = new ArrayList<>();
    for (ArrowColumnWriter currentWriter : currentWriters) {
      try {
        currentWriter.close();
        currentWriter.commit();
        logger.debug("Boosted column [{}] of split [{}]", currentWriter.column, currentWriter.splitPath);
      } catch (Exception ex) {
        logger.error("Failure while committing boost file of column [{}] of split [{}]", currentWriter.column, currentWriter.splitPath);
        failedWriters.add(currentWriter);
      }
    }

    currentWriters.removeAll(failedWriters);
    failedWriters.clear();
    currentWriters.clear();
    boostOutputContainer.setAllCount(currentWriters.size());
    return currentWriters.size();
  }

  @Override
  protected RecordReaderIterator createRecordReaderIterator() {
    recordReaderIterator = splitReaderCreatorIterator.getRecordReaderIterator();
    return recordReaderIterator;
  }

  @Override
  protected RecordReaderIterator getRecordReaderIterator() {
    return recordReaderIterator;
  }

  @Override
  protected void addSplits(List<SplitAndPartitionInfo> splits) {
    splitReaderCreatorIterator.addSplits(splits);
  }

  @Override
  protected void setSplitReaderCreatorIterator() throws IOException, ExecutionSetupException {
    splitReaderCreatorIterator = new ParquetSplitReaderCreatorIterator(fec, context, props, functionConfig, true, true);
  }

  @Override
  protected void addBoostSplits() throws IOException {
    return;
  }
}
