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
package com.dremio.exec.store.parquet;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.ScanOperator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Parquet scan table function
 */
public class ParquetScanTableFunction extends AbstractTableFunction {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanTableFunction.class);
  protected final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  private ScanOperator.ScanMutator mutator;
  private MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();
  FragmentExecutionContext fec;
  OpProps props;
  VarBinaryVector inputSplits;
  private int batchSize;
  private int currentRow;
  ParquetSplitReaderCreatorIterator splitReaderCreatorIterator;
  RecordReaderIterator recordReaderIterator;
  protected RecordReader currentRecordReader;
  BatchSchema schema;
  List<SchemaPath> selectedColumns;
  public ParquetScanTableFunction(FragmentExecutionContext fec,
                                  OperatorContext context,
                                  OpProps props,
                                  TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fec = fec;
    this.props = props;
    this.schema = functionConfig.getFunctionContext().getFullSchema();
    this.selectedColumns = functionConfig.getFunctionContext().getColumns() == null ? null : ImmutableList.copyOf(functionConfig.getFunctionContext().getColumns());
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    this.incoming = accessible;
    this.outgoing = context.createOutputVectorContainer();
    this.mutator = new ScanOperator.ScanMutator(outgoing, fieldVectorMap, context, callBack);
    schema.maskAndReorder(functionConfig.getFunctionContext().getColumns()).materializeVectors(selectedColumns, mutator);
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    callBack.getSchemaChangedAndReset();

    TypedFieldId typedFieldId = incoming.getSchema().getFieldId(SchemaPath.getSimplePath(RecordReader.SPLIT_INFORMATION));
    Field field = incoming.getSchema().getColumn(typedFieldId.getFieldIds()[0]);
    inputSplits = (VarBinaryVector) incoming.getValueAccessorById(TypeHelper.getValueVectorClass(field), typedFieldId.getFieldIds()).getValueVector();
    splitReaderCreatorIterator = new ParquetSplitReaderCreatorIterator(fec, context, props, functionConfig);
    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    currentRow = row;
    if (row != 0) {
      return;
    }

    batchSize = incoming.getRecordCount();
    if (batchSize == 0) {
      return;
    }

    // getReaders
    List<SplitAndPartitionInfo> splits = new ArrayList<>();
    for(int record=0; record<batchSize; ++record) {
      try (ByteArrayInputStream bis = new ByteArrayInputStream(inputSplits.get(record));
           ObjectInput in = new ObjectInputStream(bis)) {
        splits.add((SplitAndPartitionInfo) in.readObject());
      }
      catch (Exception e) {
        throw UserException
                .dataReadError(e)
                .message("Failed to read input split information.")
                .build(logger);
      }
    }

    try {
      recordReaderIterator = splitReaderCreatorIterator.getReaders(splits);
    } catch (Exception e) {
      ScanOperator.handleExceptionDuringScan(e, functionConfig.getFunctionContext().getReferencedTables(), logger);
    }
    setupNextReader();
  }

  void setupNextReader() throws Exception {
    OperatorStats stats = context.getStats();
    try {
      stats.startSetup();
      currentRecordReader = recordReaderIterator.next();
      checkNotNull(currentRecordReader).setup(mutator);
    } catch (Exception e) {
      ScanOperator.handleExceptionDuringScan(e, functionConfig.getFunctionContext().getReferencedTables(), logger);
    } finally {
      stats.stopSetup();
    }
    stats.addLongStat(ScanOperator.Metric.NUM_READERS, 1);
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) throws Exception {
    if (currentRow != 0) {
      return 0;
    }
    currentRecordReader.allocate(fieldVectorMap);
    int records;
    while ((records = currentRecordReader.next()) == 0) {
      currentRecordReader.close();
      currentRecordReader = null;
      if (!recordReaderIterator.hasNext()) {
        return 0;
      }

      setupNextReader();
      currentRecordReader.allocate(fieldVectorMap);
    }
    outgoing.setRecordCount(records);
    return records;
  }

  @Override
  public void closeRow() throws Exception {
    if (currentRow == 0) {
      AutoCloseables.close(currentRecordReader, recordReaderIterator);
      currentRecordReader = null;
      recordReaderIterator = null;
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    AutoCloseables.close(currentRecordReader, recordReaderIterator);
    currentRecordReader = null;
    recordReaderIterator = null;
    this.context.getStats().setReadIOStats();
  }
}
