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
package com.dremio.exec.store.deltalake;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.FileConfigMetadata;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.PhysicalDatasetUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.options.Options;
import com.dremio.sabot.exec.store.deltalake.proto.DeltaLakeProtobuf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;

/**
 * The this class is responsible for all pre-planning scans on the DeltaTable metadata. It selectively gathers
 * information, that leads to the dataset size, the relevant commit files and the schema
 */
@Options
public class DeltaLakeFormatDatasetAccessor implements FileDatasetHandle {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeFormatDatasetAccessor.class);
  private final DatasetType type;
  private final NamespaceKey tableSchemaPath;
  private final FileSelection fileSelection;
  private final FormatPlugin formatPlugin;
  private final DeltaLakeTable deltaTable;

  public DeltaLakeFormatDatasetAccessor(DatasetType type,
                                        FileSystem fs,
                                        FileSelection fileSelection,
                                        NamespaceKey tableSchemaPath,
                                        FormatPlugin formatPlugin) {

    this.type = type;
    this.fileSelection = fileSelection;
    this.tableSchemaPath = tableSchemaPath;
    this.formatPlugin = formatPlugin;
    this.deltaTable = new DeltaLakeTable(fs, fileSelection);
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(tableSchemaPath);
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException {
    try {
      final DeltaLogSnapshot snapshot = deltaTable.getConsolidatedSnapshot();

      /**
       * NOTE: This function impl is done in order to facilitate E2E validations and is not the final version.
       * Final version will be covered as part of DX-26752
       */
      return new FileConfigMetadata() {

        @Override
        public DatasetStats getDatasetStats() {
          return DatasetStats.of(snapshot.getNetOutputRows(), ScanCostFactor.PARQUET.getFactor());
        }

        @Override
        public Schema getRecordSchema() {
          try {
            Preconditions.checkNotNull(snapshot, "Unable to read commit snapshot");
            return DeltaLakeSchemaConverter.fromSchemaString(snapshot.getSchema());
          } catch (IOException e) {
            logger.error("Error while parsing DeltaLake schema", e);
            throw new RuntimeException(e);
          }
        }

        @Override
        public List<String> getPartitionColumns() {
          return snapshot.getPartitionColumns();
        }

        @Override
        public List<String> getSortColumns() {
          return new ArrayList<>(); // TODO: Implement
        }

        @Override
        public BytesOutput getExtraInfo() {
          final DeltaLakeProtobuf.DeltaLakeDatasetXAttr xAttr = deltaTable.buildDatasetXattr();
          return xAttr::writeTo;
        }

        @Override
        public FileConfig getFileConfig() {
          return PhysicalDatasetUtils.toFileFormat(formatPlugin).asFileConfig().setLocation(fileSelection.getSelectionRoot());
        }
      };
    } catch (IOException e) {
      logger.error("Error while scanning DeltaLake table", e);
      throw new ConnectorException(e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException {
    // TODO: Implement
    return new PartitionChunkListingImpl();
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    try {
      return deltaTable.readSignature();
    } catch (IOException e) {
      throw new ConnectorException(e);
    }
  }
}