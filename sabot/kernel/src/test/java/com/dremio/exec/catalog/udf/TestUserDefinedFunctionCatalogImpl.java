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
package com.dremio.exec.catalog.udf;

import static com.dremio.common.expression.CompleteType.VARBINARY;
import static com.dremio.exec.catalog.CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED;
import static com.dremio.exec.store.sys.udf.UserDefinedFunctionSerde.toProto;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.options.OptionManager;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Optional;
import org.junit.Test;

public class TestUserDefinedFunctionCatalogImpl {
  private final QueryContext queryContext = mock(QueryContext.class);
  private final OptionManager optionManager = mock(OptionManager.class);
  private final NamespaceService userNamespaceService = mock(NamespaceService.class);

  private final SourceCatalog sourceCatalog = mock(SourceCatalog.class);

  private final long created_at = 1L;
  private final long modified_at = 2L;
  private final UserDefinedFunction udf1 =
      new UserDefinedFunction(
          "test1",
          "SELECT 1",
          CompleteType.VARCHAR,
          new ArrayList<>(),
          new ArrayList<>(),
          null,
          new Timestamp(created_at),
          new Timestamp(modified_at));

  private final FunctionConfig functionConfig =
      toProto(udf1).setCreatedAt(created_at).setLastModified(modified_at);

  private UserDefinedFunctionCatalog newUDFCatalog() {
    return new UserDefinedFunctionCatalogImpl(
        queryContext, optionManager, userNamespaceService, sourceCatalog);
  }

  @Test
  public void testCreateFunction_nonVersionedSource() throws NamespaceException {
    NamespaceKey namespaceKey = new NamespaceKey("nonVersioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mock(StoragePlugin.class));

    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(namespaceKey);
    newUDFCatalog().createFunction(catalogEntityKey, userDefinedFunction);

    verify(userNamespaceService).addOrUpdateFunction(eq(namespaceKey), any());
  }

  @Test
  public void testCreateFunction_versionedSourceNotEnabled() {
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(false);
    final MockVersionedPlugin mockVersionedPlugin = mock(MockVersionedPlugin.class);

    NamespaceKey namespaceKey = new NamespaceKey("versioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mockVersionedPlugin);
    when(mockVersionedPlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(mockVersionedPlugin.unwrap(VersionedPlugin.class)).thenReturn(mockVersionedPlugin);
    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(namespaceKey);
    assertThatThrownBy(() -> newUDFCatalog().createFunction(catalogEntityKey, userDefinedFunction))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("You cannot store a user-defined function in source 'versioned'.");
    verifyNoInteractions(userNamespaceService);
  }

  @Test
  public void testCreateFunction_versionedSourceEnabled() throws IOException {
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(true);
    MockVersionedPlugin mockVersionedPlugin = mock(MockVersionedPlugin.class);

    String sourceName = "versioned";
    NamespaceKey namespaceKey = new NamespaceKey(sourceName);
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mockVersionedPlugin);
    when(mockVersionedPlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(mockVersionedPlugin.isWrapperFor(MutablePlugin.class)).thenReturn(true);
    when(mockVersionedPlugin.unwrap(MutablePlugin.class)).thenReturn(mockVersionedPlugin);
    UserSession userSession = mock(UserSession.class);
    when(queryContext.getSession()).thenReturn(userSession);
    VersionContext testBranchVersionContext = VersionContext.ofBranch("test_branch");
    when(userSession.getSessionVersionForSource(any())).thenReturn(testBranchVersionContext);
    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    CatalogEntityKey catalogEntityKey =
        CatalogEntityKey.newBuilder()
            .keyComponents(ImmutableList.of(sourceName))
            .tableVersionContext(TableVersionContext.of(testBranchVersionContext))
            .build();
    newUDFCatalog().createFunction(catalogEntityKey, userDefinedFunction);

    verify(mockVersionedPlugin).createFunction(eq(catalogEntityKey), any(), any());
    verifyNoInteractions(userNamespaceService);
  }

  @Test
  public void testGetFunction_versionedSourceEnabled() {
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(true);
    MockVersionedPlugin mockVersionedPlugin = mock(MockVersionedPlugin.class);

    NamespaceKey namespaceKey = new NamespaceKey("versioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mockVersionedPlugin);
    when(mockVersionedPlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(mockVersionedPlugin.unwrap(VersionedPlugin.class)).thenReturn(mockVersionedPlugin);
    when(mockVersionedPlugin.getFunction(any())).thenReturn(Optional.of(functionConfig));
    UserSession userSession = mock(UserSession.class);
    when(queryContext.getSession()).thenReturn(userSession);
    when(userSession.getSessionVersionForSource(any()))
        .thenReturn(VersionContext.ofBranch("test_branch"));
    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    CatalogEntityKey catalogEntityKey = CatalogEntityKey.fromNamespaceKey(namespaceKey);
    UserDefinedFunction function = newUDFCatalog().getFunction(catalogEntityKey);
    assertEquals(function.getName(), udf1.getName());
    assertEquals(function.getFunctionSql(), udf1.getFunctionSql());
    assertEquals(function.getFunctionArgsList(), udf1.getFunctionArgsList());
    assertEquals(function.getCreatedAt(), udf1.getCreatedAt());
    assertEquals(function.getModifiedAt(), udf1.getModifiedAt());
    verifyNoInteractions(userNamespaceService);
  }

  private interface MockVersionedPlugin extends VersionedPlugin, MutablePlugin {}
}
