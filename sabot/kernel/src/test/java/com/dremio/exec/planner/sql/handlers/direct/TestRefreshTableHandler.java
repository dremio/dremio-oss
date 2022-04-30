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
package com.dremio.exec.planner.sql.handlers.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DatasetCatalog;
import com.dremio.exec.planner.sql.parser.SqlRefreshTable;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.DatasetRetrievalPartitionOptions;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;

@RunWith(MockitoJUnitRunner.class)
public class TestRefreshTableHandler {
  private RefreshTableHandler refreshTableHandler;
  private RefreshTableHandler refreshTableHandlerDisabled;
  @Mock private Catalog catalog;
  @Mock private NamespaceService namespaceService;

  private static final String TABLE_NAME = "my_table";
  private static final String USER_NAME = "user";
  private static final NamespaceKey TABLE_KEY = new NamespaceKey(TABLE_NAME);

  @Before
  public void setup() throws NamespaceException {
    refreshTableHandler = new RefreshTableHandler(catalog, namespaceService, true, USER_NAME);
    refreshTableHandlerDisabled = new RefreshTableHandler(catalog, namespaceService,false, USER_NAME);

    when(catalog.resolveSingle(any(NamespaceKey.class))).thenAnswer((Answer<NamespaceKey>) invocationOnMock -> {
      NamespaceKey key = invocationOnMock.getArgument(0, NamespaceKey.class);
      if (TABLE_KEY.equals(key)) {
        return TABLE_KEY;
      }

      return null;
    });

    when(namespaceService.getDataset((any(NamespaceKey.class)))).thenReturn(null);
  }

  @Test(expected = UserException.class)
  public void toResult_files_list() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.of(SqlLiteral.createCharString("file1.txt", SqlParserPos.ZERO)),
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandler.toResult("", refreshTable);
//    Refresh Metadata for files are not supported.
//    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
//    assertFalse(result.isEmpty());
//    assertTrue(result.get(0).ok);
//
//    DatasetRetrievalOptions options = optionsCaptor.getValue();
//    assertTrue(options.deleteUnavailableDatasets());
//    assertTrue(options.forceUpdate());
//    assertTrue(options.autoPromote());
//    assertEquals(Collections.singletonList("file1.txt"), ((DatasetRetrievalFilesListOptions) options).getFilesList());
  }

  @Test(expected = UserException.class)
  public void toResult_files_mulitple_list() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.of(
        SqlLiteral.createCharString("file1.txt", SqlParserPos.ZERO),
        SqlLiteral.createCharString("file2.txt", SqlParserPos.ZERO)),
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandler.toResult("", refreshTable);
//    Refresh Metadata for files are not supported.
//    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
//    assertFalse(result.isEmpty());
//    assertTrue(result.get(0).ok);
//
//    DatasetRetrievalOptions options = optionsCaptor.getValue();
//    assertTrue(options.deleteUnavailableDatasets());
//    assertTrue(options.forceUpdate());
//    assertTrue(options.autoPromote());
//
//    List<String> expectedList = new ArrayList<>();
//    expectedList.add("file1.txt");
//    expectedList.add("file2.txt");
//    assertEquals(expectedList, ((DatasetRetrievalFilesListOptions) options).getFilesList());
  }

  @Test
  public void toResult_partition_list() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.of(SqlNodeList.of(new SqlIdentifier("year", SqlParserPos.ZERO), SqlLiteral.createCharString("2021", SqlParserPos.ZERO))));

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertTrue(options.deleteUnavailableDatasets());
    assertTrue(options.forceUpdate());
    assertTrue(options.autoPromote());

    final Map<String, String> expectedPartition = new HashMap<>();
    expectedPartition.put("year", "2021");
    assertEquals(expectedPartition, ((DatasetRetrievalPartitionOptions) options).getPartition());
  }

  @Test
  public void toResult_partition_list_null_value() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.of(SqlNodeList.of(new SqlIdentifier("year", SqlParserPos.ZERO), SqlLiteral.createNull(SqlParserPos.ZERO))));

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertTrue(options.deleteUnavailableDatasets());
    assertTrue(options.forceUpdate());
    assertTrue(options.autoPromote());

    final Map<String, String> expectedPartition = new HashMap<>();
    expectedPartition.put("year", null);
    assertEquals(expectedPartition, ((DatasetRetrievalPartitionOptions) options).getPartition());
  }

  @Test
  public void toResult_partition_mulitple_list() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.of(
        SqlNodeList.of(new SqlIdentifier("year", SqlParserPos.ZERO), SqlLiteral.createCharString("2021", SqlParserPos.ZERO)),
        SqlNodeList.of(new SqlIdentifier("month", SqlParserPos.ZERO), SqlLiteral.createCharString("Jan", SqlParserPos.ZERO))));

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertTrue(options.deleteUnavailableDatasets());
    assertTrue(options.forceUpdate());
    assertTrue(options.autoPromote());

    final Map<String, String> expectedPartition = new HashMap<>();
    expectedPartition.put("year", "2021");
    expectedPartition.put("month", "Jan");
    assertEquals(expectedPartition, ((DatasetRetrievalPartitionOptions) options).getPartition());
  }

  @Test
  public void toReseult_all_files() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.UNCHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertFalse(options.deleteUnavailableDatasets());
    assertFalse(options.forceUpdate());
    assertFalse(options.autoPromote());
    assertEquals(DatasetRetrievalOptions.class, options.getClass());
  }

  @Test
  public void toReseult_all_partitions() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.UNCHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertFalse(options.deleteUnavailableDatasets());
    assertFalse(options.forceUpdate());
    assertFalse(options.autoPromote());
    assertEquals(DatasetRetrievalOptions.class, options.getClass());
  }

  @Test
  public void toReseult_no_files() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.UNCHANGED);

    final List<SimpleCommandResult> result = refreshTableHandler.toResult("", refreshTable);
    assertFalse(result.isEmpty());
    assertTrue(result.get(0).ok);

    DatasetRetrievalOptions options = optionsCaptor.getValue();
    assertFalse(options.deleteUnavailableDatasets());
    assertFalse(options.forceUpdate());
    assertFalse(options.autoPromote());
    assertEquals(DatasetRetrievalOptions.class, options.getClass());
  }

  @Test(expected = UserException.class)
  public void toResult_files_list_disabled() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.of(SqlLiteral.createCharString("file1.txt", SqlParserPos.ZERO)),
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandlerDisabled.toResult("", refreshTable);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void toResult_partition_disabled() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.of(SqlNodeList.of(new SqlIdentifier("year", SqlParserPos.ZERO), SqlLiteral.createCharString("2021", SqlParserPos.ZERO))));

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandlerDisabled.toResult("", refreshTable);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void toResult_all_files_disabled() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandlerDisabled.toResult("", refreshTable);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void toResult_all_partitions_disabled() throws Exception {
    final SqlRefreshTable refreshTable = new SqlRefreshTable(
      SqlParserPos.ZERO,
      new SqlIdentifier(TABLE_NAME, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
      SqlNodeList.EMPTY,
      SqlNodeList.EMPTY);

    final ArgumentCaptor<DatasetRetrievalOptions> optionsCaptor = ArgumentCaptor.forClass(DatasetRetrievalOptions.class);
    lenient().when(catalog.refreshDataset(eq(TABLE_KEY), optionsCaptor.capture())).thenReturn(DatasetCatalog.UpdateStatus.CHANGED);

    refreshTableHandlerDisabled.toResult("", refreshTable);
  }
}
