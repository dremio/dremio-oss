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

import java.io.IOException;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Iceberg manifest list scan operator creator
 */
public class IcebergManifestListScanCreator implements ProducerOperator.Creator<IcebergManifestListSubScan>{
    @Override
    public ProducerOperator create(FragmentExecutionContext fragmentExecContext,
                                   OperatorContext context,
                                   IcebergManifestListSubScan config) throws ExecutionSetupException {
        final SupportsIcebergRootPointer plugin = fragmentExecContext.getStoragePlugin(config.getPluginId());
        FileSystem fs;
        try {
            fs = plugin.createFSWithAsyncOptions(config.getLocation(), config.getProps().getUserName(), context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<SplitAndPartitionInfo> splits = config.getSplits();
        IcebergProtobuf.IcebergDatasetSplitXAttr splitXAttr;
        try {
            splitXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetSplitXAttr.PARSER,
                    splits.get(0).getDatasetSplitInfo().getExtendedProperty());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Could not deserialize split info", e);
        }
        final RecordReader reader = new IcebergManifestListRecordReader(context, splitXAttr.getPath(), plugin,
                config.getTableSchemaPath(), config.getDatasourcePluginId().getName(), config.getFullSchema(), config.getProps(),
                config.getPartitionColumns(), config.getIcebergExtendedProp(), config.getManifestContent());
        return new ScanOperator(config, context, RecordReaderIterator.from(reader));
    }
}
