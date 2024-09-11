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

import static com.dremio.exec.ExecConstants.SHOW_CREATE_ENABLED;
import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.VALIDATION;
import static com.dremio.service.namespace.dataset.proto.DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER;
import static com.dremio.service.namespace.dataset.proto.DatasetType.VIRTUAL_DATASET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.planner.sql.parser.SqlShowCreate;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.test.UserExceptionAssert;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestShowCreateHandler extends TestShowCreateHandlerBase {
  private ShowCreateHandler showCreateHandler;

  @Override
  @Before
  public void setup() {
    super.setup();
    showCreateHandler = spy(new ShowCreateHandler(catalog, context));
  }

  @Test
  public void testShowCreateDisabled() throws Exception {
    testShowCreateDisabled(SHOW_CREATE_VIEW);
    testShowCreateDisabled(SHOW_CREATE_TABLE);
  }

  private void testShowCreateDisabled(SqlShowCreate sqlShowCreate) throws Exception {
    when(optionManager.getOption(SHOW_CREATE_ENABLED)).thenReturn(false);
    assertThatThrownBy(() -> showCreateHandler.toResult("", sqlShowCreate))
        .hasMessageEndingWith("syntax is disabled");
  }

  @Test
  public void testShowCreateView() throws Exception {
    doReturn(DEFAULT_VIEW_KEY).when(catalog).resolveSingle(DEFAULT_VIEW_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VIEW_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VIEW_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(VIRTUAL_DATASET);
    String sql = "SELECT * FROM foo";
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(sql);
    datasetConfig.setVirtualDataset(virtualDataset);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);

    List<ShowCreateHandler.DefinitionResult> result =
        showCreateHandler.toResult(sql, SHOW_CREATE_VIEW);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).path).isEqualTo(String.format("[%s]", DEFAULT_VIEW_KEY));
    assertThat(result.get(0).sql_definition).isEqualTo(sql);
  }

  @Test
  public void testShowCreateVersionedView() throws Exception {
    doReturn(DEFAULT_VERSIONED_VIEW_KEY).when(catalog).resolveSingle(DEFAULT_VERSIONED_VIEW_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VERSIONED_VIEW_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(
            DEFAULT_VERSIONED_VIEW_KEY.getRoot(), DEFAULT_BRANCH_VERSION_CONTEXT);
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn(DEFAULT_BRANCH_NAME);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VERSIONED_VIEW_PATH)
                .tableVersionContext(TableVersionContext.of(DEFAULT_BRANCH_VERSION_CONTEXT))
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(VIRTUAL_DATASET);
    String sql = "SELECT * FROM arctic.table";
    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql(sql);
    datasetConfig.setVirtualDataset(virtualDataset);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);

    List<ShowCreateHandler.DefinitionResult> result =
        showCreateHandler.toResult(sql, SHOW_CREATE_VERSIONED_VIEW);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).path)
        .isEqualTo(
            String.format(
                "[%s] at %s",
                DEFAULT_VERSIONED_VIEW_KEY,
                ResolvedVersionContext.convertToVersionContext(resolvedVersionContext)));
    assertThat(result.get(0).sql_definition).isEqualTo(sql);
  }

  @Test
  public void testShowCreateViewNotFound() throws Exception {
    doReturn(DEFAULT_VIEW_KEY).when(catalog).resolveSingle(DEFAULT_VIEW_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VIEW_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(DEFAULT_VIEW_KEY.getRoot(), VersionContext.NOT_SPECIFIED);
    doReturn(null)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VIEW_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());

    UserExceptionAssert.assertThatThrownBy(() -> showCreateHandler.toResult("", SHOW_CREATE_VIEW))
        .hasErrorType(VALIDATION)
        .hasMessageStartingWith(String.format("Unknown view [%s]", DEFAULT_VIEW_KEY));
  }

  @Test
  public void testShowCreateViewOnTable() throws Exception {
    doReturn(DEFAULT_VIEW_KEY).when(catalog).resolveSingle(DEFAULT_VIEW_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VIEW_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(DEFAULT_VIEW_KEY.getRoot(), VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VIEW_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);

    UserExceptionAssert.assertThatThrownBy(() -> showCreateHandler.toResult("", SHOW_CREATE_VIEW))
        .hasErrorType(VALIDATION)
        .hasMessage(String.format("[%s] is not a view", DEFAULT_VIEW_KEY));
  }

  @Test
  public void testShowCreateViewOnCorruptedView() throws Exception {
    doReturn(DEFAULT_VIEW_KEY).when(catalog).resolveSingle(DEFAULT_VIEW_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VIEW_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(DEFAULT_VIEW_KEY.getRoot(), VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VIEW_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(VIRTUAL_DATASET);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);

    UserExceptionAssert.assertThatThrownBy(() -> showCreateHandler.toResult("", SHOW_CREATE_VIEW))
        .hasErrorType(VALIDATION)
        .hasMessage(String.format("View at [%s] is corrupted", DEFAULT_VIEW_KEY));
  }

  @Test
  public void testShowCreateTableShouldReturnPrePopulatedSelectStatement() throws Exception {
    doReturn(DEFAULT_TABLE_KEY).when(catalog).resolveSingle(DEFAULT_TABLE_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_TABLE_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_TABLE_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);
    when(table.getRowType(JavaTypeFactoryImpl.INSTANCE)).thenReturn(type);
    List<RelDataTypeField> fieldList = Arrays.asList(field);
    when(type.getFieldList()).thenReturn(fieldList);
    doReturn(null)
        .when(showCreateHandler)
        .getTableDefinition(datasetConfig, DEFAULT_TABLE_KEY, fieldList, false);

    List<ShowCreateHandler.DefinitionResult> result =
        showCreateHandler.toResult("", SHOW_CREATE_TABLE);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).path).isEqualTo(String.format("[%s]", DEFAULT_TABLE_KEY));
    assertThat(result.get(0).sql_definition)
        .isEqualTo(String.format("SELECT * FROM %s", DEFAULT_TABLE_KEY));
  }

  @Test
  public void testShowCreateVersionedTable() throws Exception {
    doReturn(DEFAULT_VERSIONED_TABLE_KEY).when(catalog).resolveSingle(DEFAULT_VERSIONED_TABLE_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VERSIONED_TABLE_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(
            DEFAULT_VERSIONED_VIEW_KEY.getRoot(), DEFAULT_BRANCH_VERSION_CONTEXT);
    when(resolvedVersionContext.getType()).thenReturn(ResolvedVersionContext.Type.BRANCH);
    when(resolvedVersionContext.getRefName()).thenReturn(DEFAULT_BRANCH_NAME);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VERSIONED_TABLE_PATH)
                .tableVersionContext(TableVersionContext.of(DEFAULT_BRANCH_VERSION_CONTEXT))
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);
    doReturn(true).when(showCreateHandler).isVersioned(DEFAULT_VERSIONED_TABLE_KEY);
    when(table.getRowType(JavaTypeFactoryImpl.INSTANCE)).thenReturn(type);
    List<RelDataTypeField> fieldList = Arrays.asList(field);
    when(type.getFieldList()).thenReturn(fieldList);
    String expected =
        String.format("CREATE TABLE %s (foo INT, bar VARCHAR)", DEFAULT_VERSIONED_TABLE_KEY);
    doReturn(expected)
        .when(showCreateHandler)
        .getTableDefinition(datasetConfig, DEFAULT_VERSIONED_TABLE_KEY, fieldList, true);

    List<ShowCreateHandler.DefinitionResult> result =
        showCreateHandler.toResult("", SHOW_CREATE_VERSIONED_TABLE);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).path)
        .isEqualTo(
            String.format(
                "[%s] at %s",
                DEFAULT_VERSIONED_TABLE_KEY,
                ResolvedVersionContext.convertToVersionContext(resolvedVersionContext)));
    assertThat(result.get(0).sql_definition).isEqualTo(expected);
  }

  @Test
  public void testShowCreateTableInScratchDir() throws Exception {
    doReturn(DEFAULT_TABLE_IN_SCRATCH_KEY)
        .when(catalog)
        .resolveSingle(DEFAULT_TABLE_IN_SCRATCH_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_TABLE_IN_SCRATCH_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_TABLE_IN_SCRATCH_PATH)
                .tableVersionContext(TableVersionContext.NOT_SPECIFIED)
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);
    doReturn(false).when(showCreateHandler).isVersioned(DEFAULT_TABLE_IN_SCRATCH_KEY);
    when(table.getRowType(JavaTypeFactoryImpl.INSTANCE)).thenReturn(type);
    List<RelDataTypeField> fieldList = Arrays.asList(field);
    when(type.getFieldList()).thenReturn(fieldList);
    String expected =
        String.format("CREATE TABLE %s (foo INT, bar VARCHAR)", DEFAULT_TABLE_IN_SCRATCH_KEY);
    doReturn(expected)
        .when(showCreateHandler)
        .getTableDefinition(datasetConfig, DEFAULT_TABLE_IN_SCRATCH_KEY, fieldList, false);

    List<ShowCreateHandler.DefinitionResult> result =
        showCreateHandler.toResult("", SHOW_CREATE_TABLE_IN_SCRATCH);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).path).isEqualTo(String.format("[%s]", DEFAULT_TABLE_IN_SCRATCH_KEY));
    assertThat(result.get(0).sql_definition).isEqualTo(expected);
  }

  @Test
  public void testShowCreateTableNotFound() throws Exception {
    doReturn(DEFAULT_VERSIONED_TABLE_KEY).when(catalog).resolveSingle(DEFAULT_VERSIONED_TABLE_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VERSIONED_TABLE_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(null)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VERSIONED_TABLE_PATH)
                .tableVersionContext(TableVersionContext.of(DEFAULT_BRANCH_VERSION_CONTEXT))
                .build());

    UserExceptionAssert.assertThatThrownBy(
            () -> showCreateHandler.toResult("", SHOW_CREATE_VERSIONED_TABLE))
        .hasErrorType(VALIDATION)
        .hasMessageStartingWith(String.format("Unknown table [%s]", DEFAULT_VERSIONED_TABLE_KEY));
  }

  @Test
  public void testShowCreateTableOnView() throws Exception {
    doReturn(DEFAULT_VERSIONED_TABLE_KEY).when(catalog).resolveSingle(DEFAULT_VERSIONED_TABLE_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VERSIONED_TABLE_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VERSIONED_TABLE_PATH)
                .tableVersionContext(TableVersionContext.of(DEFAULT_BRANCH_VERSION_CONTEXT))
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(VIRTUAL_DATASET);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);

    UserExceptionAssert.assertThatThrownBy(
            () -> showCreateHandler.toResult("", SHOW_CREATE_VERSIONED_TABLE))
        .hasErrorType(VALIDATION)
        .hasMessage(String.format("[%s] is not a table", DEFAULT_VERSIONED_TABLE_KEY));
  }

  @Test
  public void testShowCreateVersionedTableWithoutColumns() throws Exception {
    doReturn(DEFAULT_VERSIONED_TABLE_KEY).when(catalog).resolveSingle(DEFAULT_VERSIONED_TABLE_KEY);
    when(context.getSession().getSessionVersionForSource(DEFAULT_VERSIONED_TABLE_KEY.getRoot()))
        .thenReturn(VersionContext.NOT_SPECIFIED);
    doReturn(resolvedVersionContext)
        .when(showCreateHandler)
        .getResolvedVersionContext(
            DEFAULT_VERSIONED_TABLE_KEY.getRoot(), DEFAULT_BRANCH_VERSION_CONTEXT);
    doReturn(table)
        .when(showCreateHandler)
        .getTable(
            CatalogEntityKey.newBuilder()
                .keyComponents(DEFAULT_VERSIONED_TABLE_PATH)
                .tableVersionContext(TableVersionContext.of(DEFAULT_BRANCH_VERSION_CONTEXT))
                .build());
    DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    when(table.getDatasetConfig()).thenReturn(datasetConfig);
    when(table.getRowType(JavaTypeFactoryImpl.INSTANCE)).thenReturn(type);
    when(type.getFieldList()).thenReturn(null);
    doReturn(true).when(showCreateHandler).isVersioned(DEFAULT_VERSIONED_TABLE_KEY);

    UserExceptionAssert.assertThatThrownBy(
            () -> showCreateHandler.toResult("", SHOW_CREATE_VERSIONED_TABLE))
        .hasErrorType(VALIDATION)
        .hasMessage(String.format("Table [%s] has no columns.", DEFAULT_VERSIONED_TABLE_KEY));
  }

  @Test
  public void testShowCreateVersionedTableWithReference() throws Exception {
    RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
    RelDataTypeField field0 =
        new RelDataTypeFieldImpl("foo", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 =
        new RelDataTypeFieldImpl("bar", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    List<RelDataTypeField> fieldList = Arrays.asList(field0, field1);
    String expected =
        String.format(
            "CREATE TABLE %s (\"%s\" INTEGER NOT NULL, \"%s\" VARCHAR NOT NULL)",
            DEFAULT_VERSIONED_TABLE_KEY, field0.getName(), field1.getName());

    DatasetConfig datasetConfig = new DatasetConfig();
    PhysicalDataset physicalDataset = new PhysicalDataset();
    datasetConfig.setType(PHYSICAL_DATASET_SOURCE_FOLDER);
    datasetConfig.setPhysicalDataset(physicalDataset);
    datasetConfig.setReadDefinition(ReadDefinition.getDefaultInstance());

    String actual =
        showCreateHandler.getTableDefinition(
            datasetConfig, DEFAULT_VERSIONED_TABLE_KEY, fieldList, true);
    assertThat(actual).isEqualTo(expected);
  }
}
