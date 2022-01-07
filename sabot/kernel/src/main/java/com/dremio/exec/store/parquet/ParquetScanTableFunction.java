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

import java.io.IOException;
import java.util.List;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

public class ParquetScanTableFunction extends ScanTableFunction {

  protected ParquetSplitReaderCreatorIterator splitReaderCreatorIterator;
  private RecordReaderIterator recordReaderIterator;

  public ParquetScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    setSplitReaderCreatorIterator();
    return super.setup(accessible);
  }

  @Override
  protected void setIcebergColumnIds(byte[] extendedProperty) {
    splitReaderCreatorIterator.setIcebergExtendedProperty(extendedProperty);
    boostBufferManager.setIcebergColumnIds(extendedProperty);
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
  protected void addBoostSplits() throws IOException {
    if (currentRecordReader != null && currentRecordReader.getColumnsToBoost() != null && !currentRecordReader.getColumnsToBoost().isEmpty()) {
      boostBufferManager.addSplit(getRecordReaderIterator().getCurrentSplitAndPartitionInfo(), getRecordReaderIterator().getCurrentSplitXAttr(), currentRecordReader.getColumnsToBoost());
    }
  }

  protected void setSplitReaderCreatorIterator() throws IOException, ExecutionSetupException {
    splitReaderCreatorIterator = new ParquetSplitReaderCreatorIterator(fec, context, props, functionConfig, false, false);
  }


  @Override
  public void close() throws Exception {
    AutoCloseables.close(super::close, splitReaderCreatorIterator, recordReaderIterator);
  }
}
