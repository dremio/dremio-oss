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
package com.dremio.exec.store.hbase;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpec;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

public class HBaseScanCreator implements ProducerOperator.Creator<HBaseSubScan>{

  @Override
  public ProducerOperator create(
      final FragmentExecutionContext fragmentExecContext,
      final OperatorContext context,
      HBaseSubScan subScan) throws ExecutionSetupException {

    final List<SchemaPath> columns = subScan.getColumns() == null ? GroupScan.ALL_COLUMNS : subScan.getColumns();

    final HBaseStoragePlugin plugin2 = fragmentExecContext.getStoragePlugin(subScan.getPluginId());

    final Iterable<RecordReader> readers = FluentIterable.from(subScan.getScans()).transform(new Function<HBaseSubScanSpec, RecordReader>(){

      @Override
      public RecordReader apply(HBaseSubScanSpec scanSpec) {
        return new HBaseRecordReader(plugin2.getConnection(), scanSpec, columns, context, false);
      }});

    return new ScanOperator(subScan, context, readers.iterator());
  }


}
