/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.common.collect.Lists;

public class HBaseScanBatchCreator implements ProducerOperator.Creator<HBaseSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseScanBatchCreator.class);

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context,
      HBaseSubScan subScan) throws ExecutionSetupException {
    List<RecordReader> readers = Lists.newArrayList();
    List<SchemaPath> columns = null;
    for(HBaseSubScan.HBaseSubScanSpec scanSpec : subScan.getRegionScanSpecList()){
      try {
        if ((columns = subScan.getColumns())==null) {
          columns = GroupScan.ALL_COLUMNS;
        }
        readers.add(new HBaseRecordReader(subScan.getStorageEngine().getConnection(), scanSpec, columns, context,
          false));
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanOperator(fragmentExecContext.getSchemaUpdater(), subScan, context, readers.iterator());
  }


}
