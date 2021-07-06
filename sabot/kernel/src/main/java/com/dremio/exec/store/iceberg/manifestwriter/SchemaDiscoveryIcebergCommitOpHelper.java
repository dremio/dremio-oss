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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * Discovers the schema from the incoming data vectors instead of config. The manifest files are kept in memory, and
 * the icebergCommitterOp is lazily initialized only at the commit time.
 */
public class SchemaDiscoveryIcebergCommitOpHelper extends IcebergCommitOpHelper {
    private VarBinaryVector schemaVector;
    private BatchSchema currentSchema = BatchSchema.EMPTY;
    private List<ManifestFile> icebergManifestFiles = new ArrayList<>();
    private List<DataFile> deletedDataFiles = new ArrayList<>();
    private List<String> partitionColumns = new ArrayList<>();

    protected SchemaDiscoveryIcebergCommitOpHelper(OperatorContext context, WriterCommitterPOP config) {
        super(context, config);
    }

    @Override
    public void setup(VectorAccessible incoming) {
        TypedFieldId schemaFieldId = RecordWriter.SCHEMA.getFieldId(SchemaPath.getSimplePath(RecordWriter.FILE_SCHEMA_COLUMN));
        schemaVector = incoming.getValueAccessorById(VarBinaryVector.class, schemaFieldId.getFieldIds()).getValueVector();

        TypedFieldId id = RecordWriter.SCHEMA.getFieldId(SchemaPath.getSimplePath(RecordWriter.ICEBERG_METADATA_COLUMN));
        icebergMetadataVector = incoming.getValueAccessorById(VarBinaryVector.class, id.getFieldIds()).getValueVector();
    }

    @Override
    public void consumeData(int records) throws Exception {
        super.consumeData(records);
        IntStream.range(0, records).filter(i -> schemaVector.isSet(i) != 0).forEach(this::consumeSchema);
    }

    private void consumeSchema(int recordIdx) {
        byte[] schemaBytes = schemaVector.get(recordIdx);
        BatchSchema schemaAtThisRow = BatchSchema.deserialize(schemaBytes);
        if (!currentSchema.equals(schemaAtThisRow)) {
            currentSchema = currentSchema.merge(schemaAtThisRow);
        }
    }

    @Override
    protected void consumeManifestFile(ManifestFile manifestFile) {
        icebergManifestFiles.add(manifestFile);

        // File system partitions follow dremio-derived nomenclature - dir[idx]. Example - dir0, dir1.. and so on.
        int existingPartitionDepth = partitionColumns.size();
        if (manifestFile.partitions().size() > existingPartitionDepth) {
            IntStream.range(existingPartitionDepth, manifestFile.partitions().size()).forEach(p -> partitionColumns.add("dir" + p));
        }
    }

    @Override
    protected void consumeDeletedDataFile(DataFile deletedDataFile) {
        deletedDataFiles.add(deletedDataFile);
    }

    @Override
    public void commit() throws Exception {
        initializeIcebergOpCommitter();
        super.commit();
        icebergManifestFiles.clear();
        deletedDataFiles.clear();
    }

    private void initializeIcebergOpCommitter() throws Exception {
        // TODO: doesn't track wait times currently. need to use dremioFileIO after implementing newOutputFile method
        IcebergModel icebergModel = config.getPlugin().getIcebergModel();
        IcebergTableProps icebergTableProps = config.getIcebergTableProps();
        switch (icebergTableProps.getIcebergOpType()) {
            case CREATE:
                icebergOpCommitter = icebergModel.getCreateTableCommitter(
                        icebergTableProps.getTableName(),
                        icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                        currentSchema,
                        partitionColumns);
                break;
            case INSERT:
                icebergOpCommitter = icebergModel.getInsertTableCommitter(icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()));
                break;
            case FULL_METADATA_REFRESH:
              icebergOpCommitter = icebergModel.getFullMetadataRefreshCommitter(
                icebergTableProps.getTableName(),
                icebergTableProps.getTableLocation(),
                icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                currentSchema,
                partitionColumns
              );
              break;
            case INCREMENTAL_METADATA_REFRESH:
                icebergOpCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(
                    icebergTableProps.getTableName(),
                    icebergTableProps.getTableLocation(),
                    icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                    currentSchema,
                    partitionColumns
                  );
                break;
        }

        try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
            icebergManifestFiles.forEach(icebergOpCommitter::consumeManifestFile);
            deletedDataFiles.forEach(icebergOpCommitter::consumeDeleteDataFile);
        }
    }
}
