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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
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
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.protobuf.ByteString;

/**
 * Helper class for flushing out commits
 */
public class IcebergCommitOpHelper implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IcebergCommitOpHelper.class);

    protected final WriterCommitterPOP config;
    protected final OperatorContext context;
    protected VarBinaryVector icebergMetadataVector;
    protected ListVector partitionDataVector;
    protected IcebergOpCommitter icebergOpCommitter;
    private final Set<IcebergPartitionData> addedPartitions = new HashSet<>(); // partitions with new files
    private final Set<IcebergPartitionData> deletedPartitions = new HashSet<>(); // partitions with deleted files
    protected Predicate<String> partitionExistsPredicate = path -> true;
    protected ReadSignatureProvider readSigProvider = (added, deleted) -> ByteString.EMPTY;

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
        partitionDataVector = (ListVector) VectorUtil.getVectorFromSchemaPath(incoming, RecordWriter.PARTITION_DATA_COLUMN);

        switch (icebergTableProps.getIcebergOpType()) {
            case CREATE:
                icebergOpCommitter = icebergModel.getCreateTableCommitter(
                        icebergTableProps.getTableName(),
                        icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                        icebergTableProps.getFullSchema(),
                        icebergTableProps.getPartitionColumnNames(),
                        context.getStats()
                );
                break;
            case INSERT:
                icebergOpCommitter = icebergModel.getInsertTableCommitter(
                        icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                        context.getStats()
                );
                break;
          case FULL_METADATA_REFRESH:
            createReadSignProvider(icebergTableProps, true);
            icebergOpCommitter = icebergModel.getFullMetadataRefreshCommitter(
                  icebergTableProps.getTableName(),
                  config.getDatasetPath().getPathComponents(),
                  icebergTableProps.getTableLocation(),
                  icebergTableProps.getUuid(),
                  icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                  icebergTableProps.getFullSchema(),
                  icebergTableProps.getPartitionColumnNames(),
                  config.getDatasetConfig().get(),
                  context.getStats()
                );
                break;
            case INCREMENTAL_METADATA_REFRESH:
              createReadSignProvider(icebergTableProps, false);
              icebergOpCommitter = icebergModel.getIncrementalMetadataRefreshCommitter(
                    context,
                    icebergTableProps.getTableName(),
                    config.getDatasetPath().getPathComponents(),
                    icebergTableProps.getTableLocation(),
                    icebergTableProps.getUuid(),
                    icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()),
                    icebergTableProps.getPersistedFullSchema(),
                    icebergTableProps.getPartitionColumnNames(),
                    false,
                    config.getDatasetConfig().get()
                );
                icebergOpCommitter.updateSchema(icebergTableProps.getFullSchema());
                break;
        }
    }

    private List<String> getPartitionPaths() {
        return config.getIcebergTableProps().getPartitionPaths();
    }

  public void consumeData(int records) throws Exception {
        for (int i = 0; i < records; ++i) {
            IcebergMetadataInformation icebergMetadataInformation = getIcebergMetadataInformation(i);
            IcebergMetadataInformation.IcebergMetadataFileType metadataFileType = icebergMetadataInformation.getIcebergMetadataFileType();
            switch (metadataFileType) {
                case MANIFEST_FILE:
                    ManifestFile manifestFile = IcebergSerDe.deserializeManifestFile(icebergMetadataInformation.getIcebergMetadataFileByte());
                    consumeManifestFile(manifestFile);
                    consumeManifestPartitionData(i);
                    break;
                case DELETE_DATAFILE:
                    DataFile dataFile = IcebergSerDe.deserializeDataFile(icebergMetadataInformation.getIcebergMetadataFileByte());
                    consumeDeletedDataFile(dataFile);
                    consumeDeletedDataFilePartitionData(i);
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
        ByteString newReadSignature = readSigProvider.compute(addedPartitions, deletedPartitions);
        icebergOpCommitter.updateReadSignature(newReadSignature);
      try (AutoCloseable ac = OperatorStats.getWaitRecorder(context.getStats())) {
            icebergOpCommitter.commit();
        }
    }

  protected void createReadSignProvider(IcebergTableProps icebergTableProps, boolean isFullRefresh) {
    if (config.isReadSignatureEnabled()) {
      ByteString existingReadSignature = null;
      if (!isFullRefresh) {
        existingReadSignature = ByteString.copyFrom(config.getDatasetConfig().get().getReadDefinition().getReadSignature().toByteArray());  // TODO avoid copy
      }
      readSigProvider = config.getSourceTablePlugin().createReadSignatureProvider(existingReadSignature, icebergTableProps.getDataTableLocation(),
        context.getFunctionContext().getContextInformation().getQueryStartTime(),
        getPartitionPaths(), partitionExistsPredicate, isFullRefresh, config.isPartialRefresh());
    }
  }

  private void consumeDeletedDataFilePartitionData(int record) {
    if (config.isReadSignatureEnabled()) {
      List<IcebergPartitionData> partitionData = getPartitionData(record);
      if (!partitionData.isEmpty()) {
        deletedPartitions.add(partitionData.get(0));
      }
    }
  }

  private void consumeManifestPartitionData(int record) {
    if (config.isReadSignatureEnabled()) {
      addedPartitions.addAll(getPartitionData(record));
    }
  }

    private List<IcebergPartitionData> getPartitionData(int i) {
      List<IcebergPartitionData> partitionDataList = new ArrayList<>();
      UnionListReader partitionDataVectorReader = partitionDataVector.getReader();
      partitionDataVectorReader.setPosition(i);
      int size = partitionDataVectorReader.size();
      FieldReader partitionDataBinaryReader = partitionDataVectorReader.reader();
      for (int j = 0; j < size; j++) {
        partitionDataBinaryReader.setPosition(j);
        byte[] bytes = partitionDataBinaryReader.readByteArray();
        IcebergPartitionData ipd = IcebergSerDe.deserializePartitionData(bytes);
        if (ipd.get(0) != null) {
          partitionDataList.add(ipd);
        }
      }
      return partitionDataList;
    }

    @Override
    public void close() throws Exception {
        icebergOpCommitter = null;
    }

    public void cleanUpManifestFiles(FileSystem fs) throws IOException, ClassNotFoundException {
        if (icebergMetadataVector == null) {
            return;
        }
        for(int i = 0; i < icebergMetadataVector.getValueCount(); i++) {
            IcebergMetadataInformation information = getIcebergMetadataInformation(i);
            if(information.getIcebergMetadataFileType() == IcebergMetadataInformation.IcebergMetadataFileType.MANIFEST_FILE) {
              Path p = Path.of(IcebergSerDe.deserializeManifestFile(information.getIcebergMetadataFileByte()).path());
              if(fs.exists(p)) {
                fs.delete(p, false);
              }
            }
        }
    }
}
