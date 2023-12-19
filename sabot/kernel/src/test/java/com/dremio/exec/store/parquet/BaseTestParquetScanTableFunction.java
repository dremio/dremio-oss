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
package com.dremio.exec.store.parquet;

import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.st;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.mockito.Mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import com.dremio.exec.store.iceberg.SupportsIcebergRootPointer;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.deletes.RowLevelDeleteFilterFactory.DeleteFileInfo;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;
import io.protostuff.ByteStringUtil;

public class BaseTestParquetScanTableFunction extends BaseTestTableFunction {

  protected FileSystem fs;
  @Mock
  protected StoragePluginId pluginId;
  @Mock(extraInterfaces = {SupportsIcebergRootPointer.class, SupportsInternalIcebergTable.class})
  protected MutablePlugin plugin;

  @Before
  public void prepareMocks() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration());
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    SupportsIcebergRootPointer sirp = (SupportsIcebergRootPointer) plugin;
    when(sirp.createFSWithAsyncOptions(any(), any(), any())).thenReturn(fs);
    SupportsInternalIcebergTable siit = (SupportsInternalIcebergTable) plugin;
    when(siit.createScanTableFunction(any(), any(), any(), any())).thenAnswer(i ->
        new ParquetScanTableFunction(i.getArgument(0), i.getArgument(1), i.getArgument(2), i.getArgument(3)));
    when(pluginId.getName()).thenReturn("testplugin");
  }

  protected RecordSet.Record inputRow(String path, long offset, long length,
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo, List<DeleteFileInfo> deleteFiles,
      ByteString extendedProps) throws Exception {
    long fileSize = fs.getFileAttributes(Path.of(path)).size();
    if (length == -1) {
      length = fileSize;
    }
    return r(
        st(path, 0L, fileSize, fileSize),
        createSplitInformation(path, offset, length, fileSize, 0, partitionInfo),
        extendedProps.toByteArray(),
        deleteFiles.stream()
            .map(info -> st(
                info.getPath(),
                info.getContent().id(),
                info.getRecordCount(),
                info.getEqualityIds()))
            .collect(Collectors.toList()));
  }

  protected TableFunctionPOP getPopForParquet(
      BatchSchema fullSchema,
      List<SchemaPath> columns,
      List<String> partitionColumns) {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.DATA_FILE_SCAN,
            false,
            new TableFunctionContext(
                getFileConfigForParquet(),
                fullSchema,
                projectedSchema,
                ImmutableList.of(ImmutableList.of("test")),
                null,
                null,
                pluginId,
                null,
                columns,
                partitionColumns,
                null,
                null,
                false,
                false,
                false,
                null)));
  }

  protected TableFunctionPOP getPopForIceberg(
      IcebergTestTables.Table table,
      BatchSchema fullSchema,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProps) {
    BatchSchema projectedSchema = fullSchema.maskAndReorder(columns);
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.DATA_FILE_SCAN,
            false,
            new TableFunctionContext(
                getFileConfigForIceberg(table),
                fullSchema,
                projectedSchema,
                ImmutableList.of(Arrays.asList(table.getTableName().split("\\."))),
                null,
                null,
                pluginId,
                null,
                columns,
                partitionColumns,
                null,
                extendedProps,
                false,
                false,
                false,
                null)));
  }

  protected FileConfig getFileConfigForParquet() {
    FileConfig config = new FileConfig();
    config.setType(FileType.PARQUET);
    return config;
  }

  protected FileConfig getFileConfigForIceberg(IcebergTestTables.Table table) {
    FileConfig config = new FileConfig();
    config.setLocation(table.getLocation());
    config.setType(FileType.ICEBERG);
    return config;
  }

  protected static ByteString getExtendedProperties(BatchSchema schema) {
    IcebergProtobuf.IcebergDatasetXAttr.Builder builder = IcebergProtobuf.IcebergDatasetXAttr.newBuilder();
    for (int i = 0; i < schema.getFields().size(); i++) {
      builder.addColumnIds(IcebergProtobuf.IcebergSchemaField.newBuilder()
          .setSchemaPath(schema.getFields().get(i).getName())
          .setId(i + 1));
    }
    return toProtostuff(builder.build()::writeTo);
  }

  protected static ByteString toProtostuff(BytesOutput out) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      out.writeTo(output);
      return ByteStringUtil.wrap(output.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected static byte[] createSplitInformation(String path, long offset, long length, long fileSize, long mtime,
      PartitionProtobuf.NormalizedPartitionInfo partitionInfo) throws Exception {
    ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
        .setPath(path)
        .setStart(offset)
        .setLength(length)
        .setFileLength(fileSize)
        .setLastModificationTime(mtime)
        .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
        .setPartitionId(partitionInfo.getId())
        .setExtendedProperty(splitExtended.toByteString());

    return IcebergSerDe.serializeToByteArray(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
  }
}
