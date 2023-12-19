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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.parser.SqlAccelToggle;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class TestAccelToggleHandler {
  private static final String TABLE_NAME_V1 = "mysource1.myfolder.mytable";
  private AccelToggleHandler accelToggleHandler;
  @Mock
  private Catalog catalog;
  @Mock
  private QueryContext queryContext;
  private DremioTable dremioTable;
  SqlLiteral raw;
  SqlLiteral enable;
  private FakeVersionedPlugin versionedPlugin;
  private ReflectionContext reflectionContext;
  private List<String> tablePath1 = Arrays.asList("mysource1", "myfolder","mytable");
  private SqlIdentifier tableIdentifier1 = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
  private NamespaceKey tableNamespaceKey1 = new NamespaceKey(tablePath1);


  @Before
  public void setup() throws NamespaceException {
    queryContext = mock(QueryContext.class, RETURNS_DEEP_STUBS);
    raw = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    enable = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    dremioTable = mock(DremioTable.class, RETURNS_DEEP_STUBS);
    versionedPlugin =  mock(FakeVersionedPlugin.class);
    reflectionContext = ReflectionContext.SYSTEM_USER_CONTEXT;
    accelToggleHandler = new AccelToggleHandler(catalog, queryContext, reflectionContext);

  }

  @Test
  public void testToggleWithVersionNoOption() throws Exception {
    SqlAccelToggle sqlAccelToggle = new SqlAccelToggle(SqlParserPos.ZERO, tableIdentifier1, raw, enable, SqlTableVersionSpec.NOT_SPECIFIED);
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath1);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey1);
    when(catalog.getSource("mysource1")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED)).thenReturn(false);
    // Act and Assert
    assertThatThrownBy(() -> accelToggleHandler.toResult("",sqlAccelToggle))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support reflection");
  }

  @Test
  public void testToggleWithVersion() throws Exception {
    SqlLiteral versionSpec = SqlLiteral.createCharString("ref1", SqlParserPos.ZERO);
    SqlAccelToggle sqlAccelToggle = new SqlAccelToggle(SqlParserPos.ZERO, tableIdentifier1, raw, enable,
      new SqlTableVersionSpec(SqlParserPos.ZERO, TableVersionType.REFERENCE, versionSpec));
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath1);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey1);
    final TableVersionContext tableVersionContext = new TableVersionContext(TableVersionType.REFERENCE, "ref1");
    when(catalog.getTable(CatalogEntityKey.newBuilder()
      .keyComponents(tablePath1)
      .tableVersionContext( tableVersionContext).build())).thenReturn(dremioTable);
    when(catalog.getSource("mysource1")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED)).thenReturn(true);

    // Act and Assert
    List<SimpleCommandResult> result = accelToggleHandler.toResult("",sqlAccelToggle);
    VersionContext expectedVersionContext = VersionContext.ofRef("ref1");
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("Acceleration enabled");
  }

  @Test
  public void testToggleWithVersionAt() throws Exception {
    SqlAccelToggle sqlAccelToggle = new SqlAccelToggle(SqlParserPos.ZERO, tableIdentifier1, raw, enable, SqlTableVersionSpec.NOT_SPECIFIED);

    final Map<String, VersionContext> sourceVersionMapping = ImmutableMap.of(
      "mysource1", VersionContext.ofBranch("branch1"),
      "mysource2", VersionContext.ofRef("ref1")
    );
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath1);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey1);
    final TableVersionContext tableVersionContext = new TableVersionContext(TableVersionType.REFERENCE, "ref1");
    CatalogEntityKey catalogEntityKey= CatalogEntityKey.newBuilder().keyComponents(tablePath1).tableVersionContext(tableVersionContext).build();
    when(catalog.getTable(CatalogEntityKey.newBuilder()
      .keyComponents(tablePath1)
      .tableVersionContext( tableVersionContext).build())).thenReturn(dremioTable);
    when(catalog.getSource("mysource1")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED)).thenReturn(true);
    when(queryContext.getSession().getSessionVersionForSource("mysource1")).thenReturn(sourceVersionMapping.get("mysource2"));
    // Act and Assert
    List<SimpleCommandResult> result = accelToggleHandler.toResult("",sqlAccelToggle);
    VersionContext expectedVersionContext = VersionContext.ofRef("ref1");
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("Acceleration enabled");
  }

  /**
   * Fake Versioned Plugin interface for test
   */
  private interface FakeVersionedPlugin extends  VersionedPlugin, StoragePlugin {
  }

}
