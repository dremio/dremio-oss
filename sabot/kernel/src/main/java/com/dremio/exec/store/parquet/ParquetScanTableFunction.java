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

import java.util.List;

import org.apache.arrow.util.VisibleForTesting;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

public class ParquetScanTableFunction extends ScanTableFunction {

  private ParquetSplitReaderCreatorIterator splitReaderCreatorIterator;

  public ParquetScanTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    splitReaderCreatorIterator = new ParquetSplitReaderCreatorIterator(fec, context, props, functionConfig);
    return super.setup(accessible);
  }

  @Override
  protected void setIcebergColumnIds(byte[] extendedProperty) {
    splitReaderCreatorIterator.setIcebergExtendedProperty(extendedProperty);
  }

  @Override
  protected RecordReaderIterator createRecordReaderIterator() {
    return splitReaderCreatorIterator.getRecordReaderIterator();
  }

  @Override
  protected void addSplits(List<SplitAndPartitionInfo> splits) {
    splitReaderCreatorIterator.addSplits(splits);
  }

  @Override
  public boolean hasBufferedRemaining() {
    this.setProduceFromBufferedSplits(true);
    splitReaderCreatorIterator.setProduceFromBufferedSplits(true);
    return splitReaderCreatorIterator.hasNext();
  }

  @Override
  protected void addRuntimeFilterToReaderIterator(RuntimeFilter runtimeFilter) {
    getSplitReaderCreatorIterator().addRuntimeFilter(runtimeFilter);
  }

  @VisibleForTesting
  ParquetSplitReaderCreatorIterator getSplitReaderCreatorIterator() {
    return this.splitReaderCreatorIterator;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(super::close, splitReaderCreatorIterator);
  }
}
