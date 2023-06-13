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

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.TableVersionType;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.ops.ReflectionContext;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlDropReflection;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class TestAccelDropHandler {
  private static final String TABLE_NAME_V1 = "mysource1.myfolder.mytable";
  private static final String TABLE_NAME_V2 = "mysource2.myfolder.mytable";
  private static final String TABLE_NAME_NV = "s3.myfolder.mytable";
  private AccelDropReflectionHandler accelDropReflectionHandler;
  @Mock
  private Catalog catalog;
  @Mock
  private QueryContext queryContext;
  private OptionManager optionManager;
  private UserSession userSession;
  private DremioTable dremioTable;
  private String layoutId = "12345";
  private SchemaUtilities.TableWithPath tableWithPath ;

  private SqlIdentifier layoutIdentifier;
  private FakeVersionedPlugin versionedPlugin;
  private StoragePlugin nonVersionedPlugin;
  private AccelerationManager accelerationManager;
  private ReflectionContext reflectionContext;


  @Before
  public void setup() throws NamespaceException {
    queryContext = mock(QueryContext.class, RETURNS_DEEP_STUBS);

    layoutIdentifier = new SqlIdentifier(layoutId, SqlParserPos.ZERO);
    dremioTable = mock(DremioTable.class, RETURNS_DEEP_STUBS);
    tableWithPath = new SchemaUtilities.TableWithPath(dremioTable);
    versionedPlugin = mock(FakeVersionedPlugin.class);
    nonVersionedPlugin = mock(StoragePlugin.class);
    accelerationManager = mock(AccelerationManager.class);
    reflectionContext = ReflectionContext.SYSTEM_USER_CONTEXT;
    accelDropReflectionHandler = new AccelDropReflectionHandler(catalog, queryContext, reflectionContext);
  }

  @Test
  public void testDropWithVersionNoOption() throws Exception {
    List<String> tablePath1 = Arrays.asList("mysource1", "myfolder","mytable");
    SqlIdentifier tableIdentifier1 = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
    NamespaceKey tableNamespaceKey1 = new NamespaceKey(tablePath1);
    SqlDropReflection sqlDropReflection = new SqlDropReflection(SqlParserPos.ZERO, tableIdentifier1, layoutIdentifier, SqlTableVersionSpec.NOT_SPECIFIED);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey1);
    when(catalog.getSource("mysource1")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_ARCTIC_ENABLED)).thenReturn(false);
    // Act and Assert
    assertThatThrownBy(() -> accelDropReflectionHandler.toResult("",sqlDropReflection))
      .isInstanceOf(UserException.class)
      .hasMessageContaining("does not support reflection");
  }

  @Test
  public void testDropOnNonVersionedSource() throws Exception {
    List<String> tablePath = Arrays.asList("s3", "myfolder","mytable");
    SqlIdentifier tableIdentifier = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
    NamespaceKey tableNamespaceKey = new NamespaceKey(tablePath);
    SqlDropReflection sqlDropReflection = new SqlDropReflection(SqlParserPos.ZERO, tableIdentifier, layoutIdentifier, SqlTableVersionSpec.NOT_SPECIFIED);
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey);
    when(catalog.getTable(tableNamespaceKey)).thenReturn(dremioTable);
    when(catalog.getSource("s3")).thenReturn(nonVersionedPlugin);
    List<SimpleCommandResult> result = accelDropReflectionHandler.toResult("",sqlDropReflection);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("Reflection dropped.");
  }

  @Test
  public void testDropOnNonVersionedSourceWithSessionVersion() throws Exception {
    List<String> tablePath = Arrays.asList("s3", "myfolder","mytable");
    SqlIdentifier tableIdentifier = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
    NamespaceKey tableNamespaceKey = new NamespaceKey(tablePath);
    SqlDropReflection sqlDropReflection = new SqlDropReflection(SqlParserPos.ZERO, tableIdentifier, layoutIdentifier, SqlTableVersionSpec.NOT_SPECIFIED);
    final Map<String, VersionContext> sourceVersionMapping = ImmutableMap.of(
      "mysource1", VersionContext.ofBranch("branch1"),
      "mysource2", VersionContext.ofRef("ref1")
    );
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey);
    when(catalog.getTable(tableNamespaceKey)).thenReturn(dremioTable);
    when(catalog.getSource("s3")).thenReturn(nonVersionedPlugin);
    when(queryContext.getSession().getSessionVersionForSource("mysource2")).thenReturn(sourceVersionMapping.get("mysource2"));
    List<SimpleCommandResult> result = accelDropReflectionHandler.toResult("",sqlDropReflection);
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
    assertThat(result.get(0).summary).contains("Reflection dropped.");
  }

  @Test
  public void testDropWithVersion() throws Exception {
    List<String> tablePath = Arrays.asList("mysource2", "myfolder","mytable");
    SqlIdentifier tableIdentifier = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
    NamespaceKey tableNamespaceKey = new NamespaceKey(tablePath);

    SqlDropReflection sqlDropReflection = new SqlDropReflection(SqlParserPos.ZERO, tableIdentifier, layoutIdentifier, SqlTableVersionSpec.NOT_SPECIFIED);

    final Map<String, VersionContext> sourceVersionMapping = ImmutableMap.of(
      "mysource1", VersionContext.ofBranch("branch1"),
      "mysource2", VersionContext.ofRef("ref1")
    );
    final TableVersionContext tableVersionContext = new TableVersionContext(TableVersionType.REFERENCE, "ref1");
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey);
    when(catalog.getTableSnapshot(tableNamespaceKey, tableVersionContext)).thenReturn(dremioTable);
    when(catalog.getSource("mysource2")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_ARCTIC_ENABLED)).thenReturn(true);
    when(queryContext.getSession().getSessionVersionForSource("mysource2")).thenReturn(sourceVersionMapping.get("mysource2"));
    // Act and Assert
    List<SimpleCommandResult> result = accelDropReflectionHandler.toResult("",sqlDropReflection);
    VersionContext expectedVersionContext = VersionContext.ofRef("ref1");
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
  }

  @Test
  public void testDropWithVersionAt() throws Exception {
    List<String> tablePath = Arrays.asList("mysource2", "myfolder","mytable");
    SqlIdentifier tableIdentifier = new SqlIdentifier(TABLE_NAME_V1, SqlParserPos.ZERO);
    SqlLiteral versionSpec = SqlLiteral.createCharString("ref1", SqlParserPos.ZERO);
    NamespaceKey tableNamespaceKey = new NamespaceKey(tablePath);

    SqlDropReflection sqlDropReflection = new SqlDropReflection(SqlParserPos.ZERO, tableIdentifier, layoutIdentifier,
      new SqlTableVersionSpec(SqlParserPos.ZERO, TableVersionType.REFERENCE, versionSpec));

    final TableVersionContext tableVersionContext = new TableVersionContext(TableVersionType.REFERENCE, "ref1");
    when(dremioTable.getPath().getPathComponents()).thenReturn(tablePath);
    when(catalog.resolveSingle(any(NamespaceKey.class))).thenReturn(tableNamespaceKey);
    when(catalog.getTableSnapshot(tableNamespaceKey, tableVersionContext)).thenReturn(dremioTable);
    when(catalog.getSource("mysource2")).thenReturn(versionedPlugin);
    when(queryContext.getOptions().getOption(CatalogOptions.REFLECTION_ARCTIC_ENABLED)).thenReturn(true);
    // Act and Assert
    List<SimpleCommandResult> result = accelDropReflectionHandler.toResult("",sqlDropReflection);
    VersionContext expectedVersionContext = VersionContext.ofRef("ref1");
    assertThat(result).isNotEmpty();
    assertThat(result.get(0).ok).isTrue();
  }


  /**
   * Fake Versioned Plugin interface for test
   */
  private interface FakeVersionedPlugin extends  VersionedPlugin, StoragePlugin {
  }

}
