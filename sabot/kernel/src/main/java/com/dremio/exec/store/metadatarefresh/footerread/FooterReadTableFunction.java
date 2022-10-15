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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.ColumnCountTooLargeException;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.FooterReaderTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.OperationType;
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
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.annotations.VisibleForTesting;

public class FooterReadTableFunction extends AbstractTableFunction {

  private static final Logger logger = LoggerFactory.getLogger(FooterReadTableFunction.class);

  private final OpProps props;
  private final FragmentExecutionContext fec;
  protected final FileType fileType;
  private FileSystem fs;
  private final SupportsInternalIcebergTable storagePlugin;
  private final String tableName;

  // inputs
  private VarCharVector pathVector;
  private BigIntVector fileSizeVector;
  private BigIntVector mtimeVector;
  private VarBinaryVector partitionDataVector;
  private BitVector isDeletedFile;

  // outputs
  private BigIntVector mtimeOutputVector;
  private VarBinaryVector dataFileVector;
  private IntVector operationTypeVector;
  private VarBinaryVector fileSchemaVector;

  private int currentRow;
  private boolean rowProcessed;
  private OperatorStats operatorStats;

  private int maxSchemaWidth = 0;
  private int numberOfRowGroupsRead = 0;

  public FooterReadTableFunction(FragmentExecutionContext fec, OperatorContext context,
                                 OpProps props, TableFunctionConfig functionConfig)  {
    super(context, functionConfig);
    this.fec = fec;
    this.props = props;
    FooterReaderTableFunctionContext functionContext = (FooterReaderTableFunctionContext) functionConfig.getFunctionContext();
    fileType = functionContext.getFileType();
    this.tableName = PathUtils.constructFullPath(functionContext.getTablePath().get(0));
    this.operatorStats = context.getStats();
    try {
      storagePlugin = fec.getStoragePlugin(functionConfig.getFunctionContext().getPluginId());
    } catch (ExecutionSetupException e) {
      throw UserException.ioExceptionError(e).buildSilently();
    }
  }

  public FileSystem getFS(String path) {
    if (fs != null) {
      return fs;
    }

    try {
      fs = storagePlugin.createFS(path, props.getUserName(), context);
      return fs;
    } catch (IOException e) {
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
    this.partitionDataVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.PARTITION_INFO);
    this.isDeletedFile = (BitVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.IS_DELETED_FILE);

    // output vectors
    this.mtimeOutputVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.MODIFICATION_TIME);
    this.dataFileVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.DATA_FILE);
    this.operationTypeVector = (IntVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.OPERATION_TYPE);
    setFileSchemaVector();

    return outgoing;
  }

  public void setFileSchemaVector() {
    this.fileSchemaVector = (VarBinaryVector) VectorUtil.getVectorFromSchemaPath(outgoing, MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.FILE_SCHEMA);
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
      DataFile dataFile;
      Optional<IcebergPartitionData> partitionData = getPartitionDataFromInput();
      String path = new String(pathVector.get(currentRow), StandardCharsets.UTF_8);
      boolean isAddedFile = !isDeletedFile.getObject(currentRow);
      long fileSize =  100;
      long recordCount =  100;
      long mTime =  -1;
      BatchSchema fileSchema =  BatchSchema.EMPTY;
      FileFormat fileFormat =  getIcebergFileFormat();
      if (isAddedFile) {
        fileSize = fileSizeVector.get(currentRow);
        if(fileSize == 0) {
          return 0; //Ignore 0 size files
        }
        Footer footer = null;
        try {
          footer = footerReader(getFS(path)).getFooter(PathUtils.withoutQueryParams(path), fileSize);
        } catch (Exception e) {
          String msg = String.format("Invalid %s footer in the dataset %s for file %s. Error - %s.", fileType, tableName, new String(pathVector.get(currentRow)), e.getMessage());
          throw UserException.validationError(e).message(msg).buildSilently();
        }
        recordCount = footer.getRowCount();
        mTime = mtimeVector.get(currentRow);
        fileSchema = footer.getSchema();

        fileSchema = fileSchema.removeNullFields();
        maxSchemaWidth = Math.max(maxSchemaWidth, fileSchema.getFieldCount());
        if(footer instanceof ParquetFooter) {
          ParquetFooter parquetFooter = (ParquetFooter) footer;
          numberOfRowGroupsRead += parquetFooter.getRowGroupCount();
        }

        if (fileSchema.getTotalFieldCount() > context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX)) {
          throw new ColumnCountTooLargeException((int) context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
        }
        fileFormat = footer.getFileFormat();
      }
      if (partitionData.isPresent()) {
        SchemaConverter schemaConverter = SchemaConverter.getBuilder().setTableName(tableName).setMapTypeEnabled(context.getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE)).build();
        List<Field> partitionColumns = partitionData.get()
                .getPartitionType()
                .fields().stream().map(x -> schemaConverter.fromIcebergColumn(x)).collect(Collectors.toList());

        fileSchema = mergePartitionColumns(fileSchema, partitionColumns);

        PartitionSpec icebergPartitionSpec = IcebergUtils.getIcebergPartitionSpec(fileSchema,
                partitionColumns.stream()
                        .map(Field::getName)
                        .collect(Collectors.toList()), null);

        dataFile = DataFiles.builder(icebergPartitionSpec)
                .withPath(path)
                .withFormat(fileFormat)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .withPartition(partitionData.get())
                .build();
      } else {
        dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path)
                .withFormat(fileFormat)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .build();
      }
      writeFileToOutput(startOutIndex, dataFile, fileSchema, mTime, isAddedFile);
    } catch (Exception e) {
      throw UserException.validationError(e).buildSilently();
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
    OperationType operationType = isAddedFile ? OperationType.ADD_DATAFILE : OperationType.DELETE_DATAFILE;

    final IcebergMetadataInformation dataFileInfo = new IcebergMetadataInformation(IcebergSerDe.serializeDataFile(dateFile));
    dataFileVector.setSafe(startOutIndex, IcebergSerDe.serializeToByteArray(dataFileInfo));
    operationTypeVector.setSafe(startOutIndex, operationType.value);

    writeFileSchemaVector(startOutIndex, schema);

    if(mtime != -1) {
      mtimeOutputVector.setSafe(startOutIndex, mtime);
    }
  }

  protected void writeFileSchemaVector(int index, BatchSchema schema) {
    if(schema != null) {
      fileSchemaVector.setSafe(index, schema.serialize());
    }
  }

  protected FooterReader footerReader(FileSystem fs) {
    switch (fileType) {
      case PARQUET:
        return new ParquetFooterReader(context, functionConfig.getTableSchema(), fs,
                0,0,true);
      default:
        throw new UnsupportedOperationException(String.format("Unknown file type - %s", fileType));
    }
  }

  private FileFormat getIcebergFileFormat() {
    switch (fileType) {
      case PARQUET:
        return FileFormat.PARQUET;
      case ORC:
        return FileFormat.ORC;
      case AVRO:
        return FileFormat.AVRO;
      default:
        throw new UnsupportedOperationException(String.format("Unknown file type - %s", fileType));
    }
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
    operatorStats.setReadIOStats();
    if(numberOfRowGroupsRead != 0) {
      operatorStats.setLongStat(ScanOperator.Metric.NUM_ROW_GROUPS, numberOfRowGroupsRead);
    }
    operatorStats.setLongStat(TableFunctionOperator.Metric.MAX_SCHEMA_WIDTH, maxSchemaWidth);
  }

  /**
   * Calculating size of first row from outgoing vectors
   * size is in bytes, 8 bytes for mtime
   * @return size of first row
   */
  @Override
  public long getFirstRowSize(){
    long firstRowSize = 8L;
    firstRowSize += fileSchemaVector.get(0).length;
    firstRowSize += dataFileVector.get(0).length;
    return firstRowSize;
  }

}
