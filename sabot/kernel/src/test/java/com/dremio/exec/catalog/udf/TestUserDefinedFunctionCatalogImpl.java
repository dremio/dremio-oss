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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.SourceCatalog;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import java.io.IOException;
import org.junit.Test;

public class TestUserDefinedFunctionCatalogImpl {
  private final OptionManager optionManager = mock(OptionManager.class);
  private final NamespaceService userNamespaceService = mock(NamespaceService.class);

  private final SourceCatalog sourceCatalog = mock(SourceCatalog.class);

  private UserDefinedFunctionCatalog newUDFCatalog() {
    return new UserDefinedFunctionCatalogImpl(optionManager, userNamespaceService, sourceCatalog);
  }

  @Test
  public void testCreateFunction_nonVersionedSource() throws IOException, NamespaceException {
    NamespaceKey namespaceKey = new NamespaceKey("nonVersioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mock(StoragePlugin.class));

    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    newUDFCatalog().createFunction(namespaceKey, userDefinedFunction);

    verify(userNamespaceService).addOrUpdateFunction(eq(namespaceKey), any());
  }

  @Test
  public void testCreateFunction_versionedSourceNotEnabled() {
    final MockVersionedPlugin mockVersionedPlugin = mock(MockVersionedPlugin.class);
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(false);

    NamespaceKey namespaceKey = new NamespaceKey("versioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot()))).thenReturn(mockVersionedPlugin);
    when(mockVersionedPlugin.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(mockVersionedPlugin.unwrap(VersionedPlugin.class)).thenReturn(mockVersionedPlugin);
    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    assertThatThrownBy(() -> newUDFCatalog().createFunction(namespaceKey, userDefinedFunction))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("You cannot store a user-defined function in source 'versioned'.");
    verifyNoInteractions(userNamespaceService);
  }

  @Test
  public void testCreateFunction_versionedSourceEnabled() throws IOException, NamespaceException {
    when(optionManager.getOption(VERSIONED_SOURCE_UDF_ENABLED)).thenReturn(true);

    NamespaceKey namespaceKey = new NamespaceKey("versioned");
    when(sourceCatalog.getSource(eq(namespaceKey.getRoot())))
        .thenReturn(mock(MockVersionedPlugin.class));

    UserDefinedFunction userDefinedFunction = mock(UserDefinedFunction.class);
    when(userDefinedFunction.getReturnType()).thenReturn(VARBINARY);

    newUDFCatalog().createFunction(namespaceKey, userDefinedFunction);

    verify(userNamespaceService).addOrUpdateFunction(eq(namespaceKey), any());
  }

  private interface MockVersionedPlugin extends VersionedPlugin, StoragePlugin {}
}
