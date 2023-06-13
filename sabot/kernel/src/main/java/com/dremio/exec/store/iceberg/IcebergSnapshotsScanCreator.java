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
package com.dremio.exec.store.iceberg;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Iceberg snapshots scan operator creator
 */
public class IcebergSnapshotsScanCreator implements ProducerOperator.Creator<IcebergSnapshotsSubScan> {

  @Override
  public ProducerOperator create(FragmentExecutionContext fragmentExecContext,
                                 OperatorContext context,
                                 IcebergSnapshotsSubScan config) throws ExecutionSetupException {
    final SupportsIcebergMutablePlugin plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
    List<SplitAndPartitionInfo> splits = config.getSplits();
    List<RecordReader> readers = splits.stream().map(s -> {
      IcebergProtobuf.IcebergDatasetSplitXAttr splitXAttr;
      try {
        splitXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetSplitXAttr.PARSER,
          s.getDatasetSplitInfo().getExtendedProperty());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not deserialize split info", e);
      }
      return new IcebergSnapshotsReader(
        context,
        splitXAttr.getPath(),
        plugin,
        config.getProps(),
        config.getIcebergTableProps(),
        config.getSnapshotsScanOptions()
      );
    }).collect(Collectors.toList());
    return new ScanOperator(config, context, RecordReaderIterator.from(readers.listIterator()));
  }
}
