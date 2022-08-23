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

import static com.dremio.exec.util.VectorUtil.getVectorFromSchemaPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.InternalIcebergUtil;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.exception.NoSupportedUpPromotionOrCoercionException;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPOP;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;

/**
 * Uses the last non-null schema from the incoming vectors. In case schema gets changed, complete manifest gets
 * re-loaded at the time of flush.
 */
public class SchemaDiscoveryManifestWritesHelper extends ManifestWritesHelper implements SupportsTypeCoercionsAndUpPromotions{
    private BatchSchema currentSchema = BatchSchema.EMPTY;
    private List<String> partitionColumns = new ArrayList<>();
    private boolean hasSchemaChanged = false;

    private List<DataFile> dataFiles = new ArrayList<>();
    private VarBinaryVector schemaVector;
    private int columnLimit;

  public SchemaDiscoveryManifestWritesHelper(IcebergManifestWriterPOP writer, int columnLimit) {
        super(writer);
        this.columnLimit = columnLimit;
  }

    @Override
    public void setIncoming(VectorAccessible incoming) {
        super.setIncoming(incoming);
        schemaVector = (VarBinaryVector) getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);
        // TODO: Setup partition info vector
    }

    @Override
    public void processIncomingRow(int recordIndex) throws IOException {
        try {
            super.processIncomingRow(recordIndex);

            if (schemaVector.isSet(recordIndex)!=0) {
                byte[] schemaSer = schemaVector.get(recordIndex);
                if (schemaSer.length==0) {
                    return;
                }
                final BatchSchema newSchema = BatchSchema.deserialize(schemaSer);
                if (newSchema.equals(currentSchema)) {
                    return;
                }

        hasSchemaChanged = true;
        try {
          currentSchema = currentSchema.mergeWithUpPromotion(newSchema, this);
        } catch (NoSupportedUpPromotionOrCoercionException e) {
          throw UserException.unsupportedError().message(e.getMessage()).build();
        }
        if (currentSchema.getTotalFieldCount() > columnLimit) {
          throw new ColumnCountTooLargeException(columnLimit);
        }
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

    @Override
    protected void addDataFile(DataFile dataFile) {
        manifestWriter.add(dataFile);
        dataFiles.add(dataFile);

        // File system partitions follow dremio-derived nomenclature - dir[idx]. Example - dir0, dir1.. and so on.
        int existingPartitionDepth = partitionColumns.size();
        if (dataFile.partition().size() > existingPartitionDepth) {
          partitionColumns = InternalIcebergUtil.getPartitionNames(dataFile);
        }
    }

    @Override
    public void startNewWriter() {
        hasSchemaChanged = false;
        dataFiles.clear();
        super.startNewWriter();
    }

    @Override
    public Optional<ManifestFile> write() throws IOException {
        addPartitionData();
        if (hasSchemaChanged) {
            deleteRunningManifestFile();
            super.startNewWriter(); // using currentSchema
            addPartitionData();
            dataFiles.stream().forEach(manifestWriter::add);
            hasSchemaChanged = false;
            currentNumDataFileAdded = dataFiles.size();
            dataFiles.clear();
        }

        return super.write();
    }

  private void addPartitionData() {
    dataFiles.stream()
      .map(DataFile::partition)
      .map(partition -> IcebergPartitionData.fromStructLike(getPartitionSpec(writer.getOptions()), partition))
      .forEach(ipd -> partitionDataInCurrentManifest().add(ipd));
  }

    @Override
    public byte[] getWrittenSchema() {
        return (currentSchema.getFieldCount() == 0) ? null : currentSchema.serialize();
    }

    @Override
    PartitionSpec getPartitionSpec(WriterOptions writerOptions) {
        Schema icebergSchema = null;
        if (writerOptions.getExtendedProperty()!=null) {
            icebergSchema = getIcebergSchema(writerOptions.getExtendedProperty(), currentSchema, writerOptions.getIcebergTableProps().getTableName());
        }

        return IcebergUtils.getIcebergPartitionSpec(currentSchema, partitionColumns, icebergSchema);
        /*
         TODO: currently we don't support partition spec update for by default spec ID will be 0. in future if
               we start supporting partition spec id. then Id must be inherited from data files(input to this writer)
         */
    }
}
