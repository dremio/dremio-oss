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

import static com.dremio.exec.catalog.CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalogImpl;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.parser.SqlShowFunctions;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestShowFunctionsHandler extends BaseTestQuery {
  private ShowFunctionsHandler showFunctionsHandler;
  @Mock private QueryContext queryContext;
  @Mock private StoragePlugin plugin;
  @Mock private Catalog catalog;
  @Mock private MockVersionedPlugin versionedPlugin;
  @Mock private UserSession userSession;
  @Mock private OptionManager optionManager;
  @Mock private NamespaceService namespaceService;
  @Mock private CatalogService catalogService;
  private UserDefinedFunctionCatalogImpl userDefinedFunctionCatalog;

  private UserDefinedFunction udf1 =
      new UserDefinedFunction(
          "test1",
          "SELECT 1",
          CompleteType.VARCHAR,
          new ArrayList<>(),
          new ArrayList<>(),
          null,
          new Timestamp(System.currentTimeMillis()),
          new Timestamp(System.currentTimeMillis()));

  private UserDefinedFunction udf2 =
      new UserDefinedFunction(
          "test2",
          "SELECT 1",
          CompleteType.VARCHAR,
          new ArrayList<>(),
          new ArrayList<>(),
          null,
          new Timestamp(System.currentTimeMillis()),
          new Timestamp(System.currentTimeMillis()));

  @Before
  public void setup() throws IOException {
    userDefinedFunctionCatalog =
        new UserDefinedFunctionCatalogImpl(
            queryContext.getSchemaConfig(),
            optionManager,
            namespaceService,
            catalogService,
            catalog);
    when(queryContext.getUserDefinedFunctionCatalog()).thenReturn(userDefinedFunctionCatalog);
    when(queryContext.getCatalog()).thenReturn(catalog);
    when(queryContext.getSession()).thenReturn(userSession);
    when(queryContext.getOptions()).thenReturn(optionManager);
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(false);
    when(catalog.getSource(anyString())).thenReturn(plugin);
    when(plugin.isWrapperFor(VersionedPlugin.class)).thenReturn(false);
    when(userSession.getDefaultSchemaPath()).thenReturn(new NamespaceKey("source"));
    showFunctionsHandler = new ShowFunctionsHandler(queryContext);
  }

  @Test
  public void testShowFunctionsWithoutVersionedPlugin() throws Exception {
    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder().keyComponents(List.of()).build();
    when(namespaceService.getFunctions())
        .thenReturn(List.of(UserDefinedFunctionSerde.toProto(udf1)));
    SqlShowFunctions showFunctions = new SqlShowFunctions(SqlParserPos.ZERO, null);
    final List<ShowFunctionsHandler.ShowFunctionResult> actualResults =
        showFunctionsHandler.toResult("foo", showFunctions);

    assertEquals(1, actualResults.size());
    assertEquals("test1", actualResults.get(0).FUNCTION_NAME);
  }

  @Test
  public void testShowFunctionsWithVersionedPlugin() throws Exception {
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(true);
    when(catalog.getSource(anyString())).thenReturn(versionedPlugin);
    when(versionedPlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(namespaceService.getFunctions())
        .thenReturn(List.of(UserDefinedFunctionSerde.toProto(udf2)));
    when(catalogService.getAllVersionedPlugins()).thenReturn(Stream.of(versionedPlugin));
    when(versionedPlugin.getFunctions(any()))
        .thenReturn(List.of(UserDefinedFunctionSerde.toProto(udf1)));
    when(userSession.getSessionVersionForSource(anyString()))
        .thenReturn(VersionContext.ofBranch("main"));

    SqlShowFunctions showFunctions = new SqlShowFunctions(SqlParserPos.ZERO, null);
    final List<ShowFunctionsHandler.ShowFunctionResult> actualResults =
        showFunctionsHandler.toResult("foo", showFunctions);

    assertEquals(2, actualResults.size());
  }

  private interface MockVersionedPlugin extends VersionedPlugin, MutablePlugin, StoragePlugin {}
}
