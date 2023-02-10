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
package com.dremio.exec.planner.sql;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.iceberg.PartitionSpec;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.TableFormatWriterOptions.TableFormatOperation;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.direct.CreateEmptyTableHandler;
import com.dremio.exec.planner.sql.handlers.query.CreateTableHandler;
import com.dremio.exec.planner.sql.parser.DremioSqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.planner.sql.parser.SqlCreateEmptyTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergCommand;
import com.dremio.exec.store.iceberg.model.IcebergTableCreationCommitter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public class TestCreateTableQueryCleanup {
  @Test
  public void testTableCleanupInIcebergTableCreationCommitterOnFailure() {
    IcebergCommand command = mock(IcebergCommand.class);
    when(command.endTransaction()).thenThrow(new UncheckedIOException(new IOException("endTransaction_error")));

    IcebergTableCreationCommitter committer = new IcebergTableCreationCommitter("table1", BatchSchema.EMPTY,
      ImmutableList.of(), command, null, PartitionSpec.unpartitioned());

    assertThatThrownBy(committer::commit)
      .isInstanceOf(RuntimeException.class)
      .hasCauseInstanceOf(UncheckedIOException.class)
      .hasMessageContaining("endTransaction_error");

    verify(command, times(1)).deleteTable();
  }

  @Test
  public void testTableCleanupInCreateEmptyTableHandlerOnFailure() throws Exception {
    Catalog catalog = mock(Catalog.class);
    NamespaceKey key = mock(NamespaceKey.class);
    String sql = "CREATE TABLE table1(c int)";
    SqlCreateEmptyTable sqlCreateEmptyTable = mock(SqlCreateEmptyTable.class);
    when(sqlCreateEmptyTable.getFieldList()).thenReturn(SqlNodeList.EMPTY);
    when(sqlCreateEmptyTable.getPartitionTransforms(null)).thenReturn(new ArrayList<>());
    SqlHandlerConfig config = mock(SqlHandlerConfig.class);
    QueryContext context = mock(QueryContext.class);
    OptionManager manager = mock(OptionManager.class);
    when(context.getOptions()).thenReturn(manager);
    when(config.getContext()).thenReturn(context);
    UserSession userSession = mock(UserSession.class);
    CreateEmptyTableHandler handler =  new CreateEmptyTableHandler(catalog, config, userSession, false);

    List<DremioSqlColumnDeclaration> columnDeclarations = SqlHandlerUtil.columnDeclarationsFromSqlNodes(SqlNodeList.EMPTY, sql);
    BatchSchema batchSchema = SqlHandlerUtil.batchSchemaFromSqlSchemaSpec(config, columnDeclarations, sql);
    PartitionSpec partitionSpec = IcebergUtils.getIcebergPartitionSpecFromTransforms(batchSchema, new ArrayList<>(), null);

    ByteString partitionSpecByteString = ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpec));
    String schemaAsJson = IcebergSerDe.serializedSchemaAsJson(SchemaConverter.getBuilder().build().toIcebergSchema(batchSchema));
    IcebergTableProps icebergTableProps = new IcebergTableProps(partitionSpecByteString, schemaAsJson);
    IcebergWriterOptions icebergWriterOptions = new ImmutableIcebergWriterOptions.Builder()
      .setIcebergTableProps(icebergTableProps).build();
    TableFormatWriterOptions tableFormatOptions = new ImmutableTableFormatWriterOptions.Builder()
      .setIcebergSpecificOptions(icebergWriterOptions).setOperation(TableFormatOperation.CREATE).build();

    WriterOptions options = new WriterOptions(0, new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(), PartitionDistributionStrategy.UNSPECIFIED,
      null, sqlCreateEmptyTable.isSingleWriter(), Long.MAX_VALUE, tableFormatOptions, null);

    doThrow(new RuntimeException("createEmptyTable_error")).when(catalog).createEmptyTable(key, batchSchema, options);

    when(catalog.getSource(key.getRoot())).thenReturn(mock(FileSystemPlugin.class));

    assertThatThrownBy(() -> handler.callCatalogCreateEmptyTableWithCleanup(key, batchSchema, options))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("createEmptyTable_error")   // Message that should be checked for: createEmptyTable_error
      .hasRootCauseInstanceOf(RuntimeException.class)
      .hasRootCauseMessage("createEmptyTable_error");
    verify(catalog, times(1)).forgetTable(key);
  }

  @Test
  public void testCTASCleanupInCreateTableHandler() throws IOException {
    final String tableFolderToDelete = "dummyTableFolderToDelete";
    final NamespaceKey tableName = new NamespaceKey(ImmutableList.of("dummyTable"));

    DatasetCatalog datasetCatalog = mock(DatasetCatalog.class);
    DremioFileIO dremioFileIO = mock(DremioFileIO.class);

    when(dremioFileIO.getPlugin()).thenReturn(mock(FileSystemPlugin.class));
    CreateTableHandler.cleanUpImpl(dremioFileIO, datasetCatalog, tableName, tableFolderToDelete);
    verify(dremioFileIO,times(1)).deleteFile(tableFolderToDelete, true, true);
    verify(datasetCatalog, times(1)).forgetTable(tableName);

    when(dremioFileIO.getPlugin()).thenReturn(mock(MutablePlugin.class));
    CreateTableHandler.cleanUpImpl(dremioFileIO, datasetCatalog, tableName, tableFolderToDelete);
    verify(dremioFileIO,times(1)).deleteFile(tableFolderToDelete, true, false);
    verify(datasetCatalog, times(1)).dropTable(tableName, null);
  }
}
