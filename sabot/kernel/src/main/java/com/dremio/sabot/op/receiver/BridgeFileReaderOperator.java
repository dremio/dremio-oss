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
package com.dremio.sabot.op.receiver;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.config.BridgeFileReader;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.BatchStreamProvider;
import com.dremio.sabot.op.spi.ProducerOperator;

/** Impl for sender operator that reads from a file, instead of a socket. */
public class BridgeFileReaderOperator extends AbstractBridgeReaderOperator {
  public enum Metric implements MetricDef {
    BYTES_READ;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public BridgeFileReaderOperator(
      BatchStreamProvider streams,
      OperatorContext context,
      BatchSchema batchSchema,
      String bridgeSetId) {
    super(streams, context, batchSchema, bridgeSetId);
  }

  @Override
  void updateMetrics(long bytesRead) {
    getStats().addLongStat(Metric.BYTES_READ, bytesRead);
  }

  public static class Creator implements ReceiverCreator<BridgeFileReader> {
    @Override
    public ProducerOperator create(
        BatchStreamProvider streams, OperatorContext context, BridgeFileReader config)
        throws ExecutionSetupException {

      return new BridgeFileReaderOperator(
          streams, context, config.getFullSchema(), config.getBridgeSetId());
    }
  }
}
