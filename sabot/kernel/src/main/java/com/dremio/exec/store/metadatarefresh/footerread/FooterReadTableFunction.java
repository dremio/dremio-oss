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
package com.dremio.exec.store.metadatarefresh.footerread;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.dfs.AbstractTableFunction;
import com.dremio.exec.store.iceberg.IcebergMetadataInformation;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.annotations.VisibleForTesting;

public class FooterReadTableFunction extends AbstractTableFunction {

  private static final Logger logger = LoggerFactory.getLogger(FooterReadTableFunction.class);

  private final OpProps props;
  private final FragmentExecutionContext fec;
  private FileSystem fs;

  // inputs
  private VarCharVector pathVector;
  private BigIntVector fileSizeVector;
  private BigIntVector mtimeVector;
  private VarBinaryVector partitionDataVector;
  private BitVector isDeletedFile;

  // outputs
  private BigIntVector mtimeOutputVector;
  private VarBinaryVector dataFileVector;
  private VarBinaryVector fileSchemaVector;

  private int currentRow;
  private boolean rowProcessed;

  public FooterReadTableFunction(FragmentExecutionContext fec, OperatorContext context,
                                 OpProps props, TableFunctionConfig functionConfig) {
    super(context, functionConfig);
    this.fec = fec;
    this.props = props;
  }

  public FileSystem getFS(String path) {
    if (fs != null) {
      return fs;
    }

    try {
      fs = ((SupportsInternalIcebergTable) fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId()))
        .createFS(path, props.getUserName(), context);
      return fs;
    } catch (IOException | ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  @VisibleForTesting
  void setFs(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);

    // input vectors
    this.pathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    this.fileSizeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE);
    this.mtimeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
    this.partitionDataVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    this.isDeletedFile = (BitVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.isDeletedFile);

    // output vectors
    this.mtimeOutputVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.MODIFICATION_TIME);
    this.dataFileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);
    this.fileSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);

    return outgoing;
  }

  @Override
  public void startRow(int row) throws Exception {
    this.rowProcessed = false;
    this.currentRow = row;
  }

  @Override
  public int processRow(int startOutIndex, int maxRecords) {
    if (rowProcessed) {
      return 0;
    }

    try {
      if(isDeletedFile.getObject(currentRow)) {
        DataFile dataFile;
        String path = new String(pathVector.get(currentRow), StandardCharsets.UTF_8);
        //If file is deleted no need to read Footer
        dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath(path)
          .withFileSizeInBytes(100)
          .withRecordCount(100)
          .withPartition(null)
          .build();
        writeFileToOutput(startOutIndex, dataFile, null, -1, false);
      } else {
        DataFile dataFile;
        String path = new String(pathVector.get(currentRow), StandardCharsets.UTF_8);
        path = URLDecoder.decode(path, StandardCharsets.UTF_8.displayName());

        long fileSize = fileSizeVector.get(currentRow);
        long mtime = mtimeVector.get(currentRow);

        Footer footer = footerReader(getFS(path)).getFooter(path, fileSize);

        BatchSchema fileSchema = footer.getSchema();

        Optional<IcebergPartitionData> partitionData = getPartitionDataFromInput();
        List<Field> partitionColumns;
        if (partitionData.isPresent()) {

          partitionColumns = partitionData.get()
            .getPartitionType()
            .fields().stream().map(SchemaConverter::fromIcebergColumn).collect(Collectors.toList());

          fileSchema = mergePartitionColumns(footer.getSchema(), partitionColumns);

          PartitionSpec icebergPartitionSpec = IcebergUtils.getIcebergPartitionSpec(fileSchema,
            partitionColumns.stream()
              .map(Field::getName)
              .collect(Collectors.toList()), null);

          dataFile = DataFiles.builder(icebergPartitionSpec)
            .withPath(path)
            .withFormat(footer.getFileFormat())
            .withFileSizeInBytes(fileSize)
            .withRecordCount(footer.getRowCount())
            .withPartition(partitionData.get())
            .build();
        } else {
          dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(path)
            .withFormat(footer.getFileFormat())
            .withFileSizeInBytes(fileSize)
            .withRecordCount(footer.getRowCount())
            .build();
        }

        writeFileToOutput(startOutIndex, dataFile, fileSchema, mtime, true);
      }
    } catch (Exception e) {
      logger.error("Ignoring file [{}]. Error while decoding footer of file",
        new String(pathVector.get(currentRow), StandardCharsets.UTF_8), e);
      return 0;
    } finally {
      rowProcessed = true;
    }
    outgoing.forEach(vw -> vw.getValueVector().setValueCount(startOutIndex + 1));
    outgoing.setRecordCount(startOutIndex + 1);
    return 1;
  }

  protected BatchSchema mergePartitionColumns(BatchSchema schema, List<Field> partitionColumns) {
    return schema.merge(new BatchSchema(partitionColumns));
  }

  private void writeFileToOutput(int startOutIndex, DataFile dateFile, BatchSchema schema, long mtime, boolean isAddedFile) throws IOException {
    IcebergMetadataInformation.IcebergMetadataFileType icebergMetadataType = isAddedFile ? IcebergMetadataInformation.IcebergMetadataFileType.ADD_DATAFILE : IcebergMetadataInformation.IcebergMetadataFileType.DELETE_DATAFILE;

    final IcebergMetadataInformation dataFileInfo = new IcebergMetadataInformation(IcebergSerDe.serializeDataFile(dateFile),
      icebergMetadataType);
    dataFileVector.setSafe(startOutIndex, IcebergSerDe.serializeToByteArray(dataFileInfo));

    if(schema != null) {
      fileSchemaVector.setSafe(startOutIndex, schema.serialize());
    }

    if(mtime != -1) {
      mtimeOutputVector.setSafe(startOutIndex, mtime);
    }
  }

  private FooterReader footerReader(FileSystem fs) {
    // TODO: Support for other fileType like Orc etc
    FileConfig fileConfig = functionConfig.getFunctionContext().getFormatSettings();
    FileType type = fileConfig == null ? FileType.PARQUET : fileConfig.getType();
    if (type == FileType.PARQUET) {
      return new ParquetFooterReader(context, functionConfig.getTableSchema(), fs);
    }
    throw new UnsupportedOperationException(String.format("Unknown file type - %s", type));
  }

  protected Optional<IcebergPartitionData> getPartitionDataFromInput() {
    if (partitionDataVector.isNull(currentRow)) {
      return Optional.empty();
    }
    byte[] partitionDataBytes = partitionDataVector.get(currentRow);
    return Optional.ofNullable(IcebergSerDe.deserializePartitionData(partitionDataBytes));
  }

  @Override
  public void closeRow() throws Exception {
  }

}
