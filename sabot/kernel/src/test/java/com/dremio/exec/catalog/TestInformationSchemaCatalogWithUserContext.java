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
package com.dremio.exec.catalog;

import static com.dremio.exec.ExecConstants.VERSIONED_INFOSCHEMA_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceService;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/** Test information schema informationSchemaCatalog with user context. */
@ExtendWith(MockitoExtension.class)
public class TestInformationSchemaCatalogWithUserContext {
  private static final String expectedUserId = UUID.randomUUID().toString();

  @Mock private NamespaceService namespaceService;

  @Mock private PluginRetriever pluginRetriever;

  @Mock private OptionManager optionManager;

  @Mock private NamespaceIdentity identity;

  @Mock private VersionedPlugin versionedPlugin;

  @InjectMocks private InformationSchemaCatalogImpl informationSchemaCatalog;

  @BeforeEach
  public void setUp() throws Exception {
    when(pluginRetriever.getAllVersionedPlugins()).thenReturn(Stream.of(versionedPlugin));
    when(optionManager.getOption(VERSIONED_INFOSCHEMA_ENABLED)).thenReturn(true);
    when(identity.getId()).thenReturn(expectedUserId);

    when(versionedPlugin.getName()).thenReturn("SomeSource");
  }

  @Test
  public void listSchemata() throws NamespaceException {
    when(versionedPlugin.getAllInformationSchemaSchemataInfo(any()))
        .thenAnswer(
            (v) -> {
              final UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
              // Assert
              assertThat(userContext.getUserId()).isEqualTo(expectedUserId);
              return Stream.empty();
            });

    informationSchemaCatalog.listSchemata(null);
  }

  @Test
  public void listTables() throws NamespaceException {
    when(versionedPlugin.getAllInformationSchemaTableInfo(any()))
        .thenAnswer(
            (v) -> {
              final UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
              // Assert
              assertThat(userContext.getUserId()).isEqualTo(expectedUserId);
              return Stream.empty();
            });

    informationSchemaCatalog.listTables(null);
  }

  @Test
  public void listViews() throws NamespaceException {
    when(versionedPlugin.getAllInformationSchemaViewInfo(any()))
        .thenAnswer(
            (v) -> {
              final UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
              // Assert
              assertThat(userContext.getUserId()).isEqualTo(expectedUserId);
              return Stream.empty();
            });

    informationSchemaCatalog.listViews(null);
  }

  @Test
  public void listColumns() throws NamespaceException {
    when(versionedPlugin.getAllInformationSchemaColumnInfo(any()))
        .thenAnswer(
            (v) -> {
              final UserContext userContext = RequestContext.current().get(UserContext.CTX_KEY);
              // Assert
              assertThat(userContext.getUserId()).isEqualTo(expectedUserId);
              return Stream.empty();
            });

    informationSchemaCatalog.listTableSchemata(null);
  }
}
