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
package com.dremio.plugins.sysflight;

import org.apache.arrow.flight.Ticket;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

/**
 * This class creates batches based on the the type of {@link SysFlightTable}.
 */
public class SysFlightScanCreator implements ProducerOperator.Creator<SysFlightSubScan> {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(
    SysFlightScanCreator.class);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public ProducerOperator create(FragmentExecutionContext fec, OperatorContext context, SysFlightSubScan config)
    throws ExecutionSetupException {
    final SysFlightStoragePlugin plugin = fec.getStoragePlugin(config.getPluginId());
    final RecordReader reader = new SysFlightRecordReader(context, config.getColumns(), config.getFullSchema(),
      plugin.getFlightClient(), new Ticket(config.getTicket().toByteArray()));

    return new ScanOperator(config, context, RecordReaderIterator.from(reader));
  }
}
