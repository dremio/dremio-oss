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
package com.dremio.exec.store.iceberg.manifestwriter;

import java.io.IOException;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * Helper class for flushing out commits
 */
public class IcebergCommitOpHelper {
    private static final Logger logger = LoggerFactory.getLogger(IcebergCommitOpHelper.class);

    protected final WriterCommitterPOP config;
    protected final OperatorContext context;
    protected VarBinaryVector icebergMetadataVector;
    protected IcebergOpCommitter icebergOpCommitter;

    public static IcebergCommitOpHelper getInstance(OperatorContext context, WriterCommitterPOP config) {
        if (config.getIcebergTableProps() == null) {
            return new NoOpIcebergCommitOpHelper(context, config);
        } else if (config.getIcebergTableProps().isDetectSchema()) {
            return new SchemaDiscoveryIcebergCommitOpHelper(context, config);
        } else {
            return new IcebergCommitOpHelper(context, config);
        }
    }

    protected IcebergCommitOpHelper(OperatorContext context, WriterCommitterPOP config) {
        this.config = config;
        this.context = context;
    }

    public void setup(VectorAccessible incoming) {
        // TODO: doesn't track wait times currently. need to use dremioFileIO after implementing newOutputFile method
        IcebergModel icebergModel = config.getPlugin().getIcebergModel();

        IcebergTableProps icebergTableProps = config.getIcebergTableProps();
        TypedFieldId id = RecordWriter.SCHEMA.getFieldId(SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA_COLUMN));
        icebergMetadataVector = incoming.getValueAccessorById(VarBinaryVector.class, id.getFieldIds()).getValueVector();

        switch (icebergTableProps.getIcebergOpType()) {
            case CREATE:
                icebergOpCommitter = icebergModel.getCreateTableCommitter(
                        icebergTableProps.getTableName(),
                        icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                        icebergTableProps.getFullSchema(),
                        icebergTableProps.getPartitionColumnNames());
                break;
            case INSERT:
                icebergOpCommitter = icebergModel.getInsertTableCommitter(
                        icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()));
                break;
          case FULL_METADATA_REFRESH:
                icebergOpCommitter = icebergModel.getFullMetadataRefreshCommitter(
                  icebergTableProps.getTableName(),
                  icebergTableProps.getTableLocation(), icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                  icebergTableProps.getFullSchema(),
                  icebergTableProps.getPartitionColumnNames()
                );
                break;
            case INCREMENTAL_METADATA_REFRESH:
                icebergOpCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(
                    icebergTableProps.getTableName(),
                    icebergTableProps.getTableLocation(), icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                    icebergTableProps.getFullSchema(),
                    icebergTableProps.getPartitionColumnNames()
                  );
                break;
        }
    }

    public void consumeData(int records) throws Exception {
        for (int i = 0; i < records; ++i) {
            IcebergMetadataInformation icebergMetadataInformation = getIcebergMetadataInformation(i);
            IcebergMetadataInformation.IcebergMetadataFileType metadataFileType = icebergMetadataInformation.getIcebergMetadataFileType();
            switch (metadataFileType) {
                case MANIFEST_FILE:
                    ManifestFile manifestFile = IcebergSerDe.deserializeManifestFile(icebergMetadataInformation.getIcebergMetadataFileByte());
                    consumeManifestFile(manifestFile);
                    break;
                case DELETE_DATAFILE:
                    DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());
                    consumeDeletedDataFile(dataFile);
                    break;
                default:
                    throw new Exception("Unsupported File Type: " + metadataFileType);
            }
        }
    }

    protected void consumeManifestFile(ManifestFile manifestFile) {
        icebergOpCommitter.consumeManifestFile(manifestFile);
    }

    protected void consumeDeletedDataFile(DataFile deletedDataFile) {
        icebergOpCommitter.consumeDeleteDataFile(deletedDataFile);
    }

    protected IcebergMetadataInformation getIcebergMetadataInformation(int row) throws IOException, ClassNotFoundException {
        return IcebergSerDe.deserializeFromByteArray(icebergMetadataVector.get(row));
    }

    public void commit() throws Exception {
        if (icebergOpCommitter == null) {
            logger.warn("Skipping iceberg commit because opCommitter isn't initialized");
            return;
        }
        try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
            icebergOpCommitter.commit();
        }
    }

    public void close() {
        icebergOpCommitter = null;
    }
}
