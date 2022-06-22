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
package com.dremio.exec.store.hive;

import static com.dremio.exec.ExecConstants.STORE_ACCURATE_PARTITION_STATS;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.hive.orc.HiveOrcFooterReader;
import com.dremio.exec.store.metadatarefresh.footerread.AvroRowCountEstimater;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReader;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;

/**
 * Footer reader table function for hive flow
 */
public class HiveFooterReaderTableFunction extends FooterReadTableFunction {

  public HiveFooterReaderTableFunction(FragmentExecutionContext fec, OperatorContext context, OpProps props, TableFunctionConfig functionConfig) {
    super(fec, context, props, functionConfig);
  }

  @Override
  protected FooterReader footerReader(FileSystem fs) {
    switch (fileType) {
      case PARQUET:
        return HiveParquetFooterReader.getInstance(context, functionConfig.getTableSchema(), fs);
      case ORC:
        return new HiveOrcFooterReader(functionConfig.getTableSchema(), fs, context);
      case AVRO:
        return new AvroRowCountEstimater(functionConfig.getTableSchema(), context);
      default:
        throw new UnsupportedOperationException(String.format("Unknown file type - %s", fileType));
    }
  }

  @Override
  public void setFileSchemaVector() {
    // No-OP
  }

  @Override
  protected void writeFileSchemaVector(int index, BatchSchema schema) {
    // No-OP
  }

  @Override
  public long getFirstRowSize(){
    return -1L;
  }

  public static boolean isAccuratePartitionStatsNeeded(OperatorContext context) {
    return context.getOptions().getOption(STORE_ACCURATE_PARTITION_STATS);
  }
}
