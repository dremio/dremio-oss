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

package com.dremio.service.autocomplete.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import javax.inject.Provider;

import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.EntityExplorer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.autocomplete.catalog.mock.MockDremioTableFactory;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.FolderConfig;
import com.dremio.service.namespace.space.proto.HomeConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class AutocompleteSchemaProviderTest {

  private static final String USER = "testUser";
  private static final String USER_HOME = "@" + USER;

  private final NamespaceService namespaceService = Mockito.mock(NamespaceService.class);
  private final ConnectionReader connectionReader = Mockito.mock(ConnectionReader.class);
  private final EntityExplorer entityExplorer = Mockito.mock(EntityExplorer.class);

  @Before
  public void setup() throws NamespaceException {
    // Let's have home space for the current user
    when(namespaceService.getHome(new NamespaceKey(USER_HOME)))
      .thenReturn(new HomeConfig());
  }

  @Test
  public void shouldFetchRootLevelNodesAndFilterInternalSourcesIfPathEmptyAndNoContextPresent() {
    when(namespaceService.getSpaces())
      .thenReturn(ImmutableList.of(space("space one"), space("space two")));

    when(namespaceService.getSources())
      .thenReturn(ImmutableList.of(source("source one"), source("source two")));

    // Let's make first to be internal and second external configs
    when(connectionReader.getConnectionConf(sourceMatching("source one")))
      .thenReturn(internalConfig());
    when(connectionReader.getConnectionConf(sourceMatching("source two")))
      .thenReturn(externalConfig());

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of());

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of());

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node(USER_HOME, Node.Type.HOME),
      new Node("space one", Node.Type.SPACE),
      new Node("space two", Node.Type.SPACE),
      new Node("source two", Node.Type.SOURCE)
    );
  }

  @Test
  public void shouldFetchRootLevelNodesAndFilterInternalSourcesIfPathOnlyHasCursorAndNoContextPresent() {
    when(namespaceService.getSpaces())
      .thenReturn(ImmutableList.of(space("space one"), space("space two")));

    when(namespaceService.getSources())
      .thenReturn(ImmutableList.of(source("source one"), source("source two")));

    // Let's make first to be internal and second external configs
    when(connectionReader.getConnectionConf(sourceMatching("source one")))
      .thenReturn(internalConfig());
    when(connectionReader.getConnectionConf(sourceMatching("source two")))
      .thenReturn(externalConfig());

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of());

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of(Cursor.CURSOR_CHARACTER));

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node(USER_HOME, Node.Type.HOME),
      new Node("space one", Node.Type.SPACE),
      new Node("space two", Node.Type.SPACE),
      new Node("source two", Node.Type.SOURCE)
    );
  }

  @Test
  public void shouldLookupNodesInCurrentContextFirst() throws NamespaceException {
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(ImmutableList.of(
        folderContainer("folder inner"),
        spaceContainer("space inner"),
        sourceContainer("source inner"),
        physicalDatasetContainer("pds inner"),
        virtualDatasetContainer("vds inner")
      ));

    // Let's define outer ones to make sure they are not being included.
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("path"))))
      .thenReturn(ImmutableList.of(
        folderContainer("folder outer"),
        spaceContainer("space outer"),
        sourceContainer("source outer"),
        physicalDatasetContainer("pds outer"),
        virtualDatasetContainer("vds outer")
      ));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of("path"));

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node("space inner", Node.Type.SPACE),
      new Node("source inner", Node.Type.SOURCE),
      new Node("folder inner", Node.Type.FOLDER),
      new Node("pds inner", Node.Type.PHYSICAL_SOURCE),
      new Node("vds inner", Node.Type.VIRTUAL_SOURCE)
    );
  }

  @Test
  public void shouldFallbackToGlobalContextWhenLookingUpSchemaAndNothingExistsInCurrentContext() throws NamespaceException {
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenThrow(new RuntimeException("Inner Context doesn't exist"));

    when(namespaceService.list(new NamespaceKey(ImmutableList.of("path"))))
      .thenReturn(ImmutableList.of(
        folderContainer("folder outer"),
        spaceContainer("space outer"),
        sourceContainer("source outer"),
        physicalDatasetContainer("pds outer"),
        virtualDatasetContainer("vds outer")
      ));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of("path"));

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node("space outer", Node.Type.SPACE),
      new Node("source outer", Node.Type.SOURCE),
      new Node("folder outer", Node.Type.FOLDER),
      new Node("pds outer", Node.Type.PHYSICAL_SOURCE),
      new Node("vds outer", Node.Type.VIRTUAL_SOURCE)
    );
  }

  @Test
  public void shouldIncludeInternalSourcesIfTheyAreReferencedExplicitly() throws NamespaceException {
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(ImmutableList.of(
        sourceContainer("source one"),
        sourceContainer("source two")
      ));


    // Let's make both sources internal
    when(connectionReader.getConnectionConf(sourceMatching("source one")))
      .thenReturn(internalConfig());
    when(connectionReader.getConnectionConf(sourceMatching("source two")))
      .thenReturn(externalConfig());

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of("path"));

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node("source one", Node.Type.SOURCE),
      new Node("source two", Node.Type.SOURCE)
    );
  }

  @Test
  public void shouldIgnoreCursorCharacterWhenReturningChildNodes() throws NamespaceException {
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(ImmutableList.of(
        folderContainer("folder inner"),
        spaceContainer("space inner"),
        sourceContainer("source inner"),
        physicalDatasetContainer("pds inner"),
        virtualDatasetContainer("vds inner")
      ));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of("path", Cursor.CURSOR_CHARACTER));

    assertThat(nodes).containsExactlyInAnyOrder(
      new Node("space inner", Node.Type.SPACE),
      new Node("source inner", Node.Type.SOURCE),
      new Node("folder inner", Node.Type.FOLDER),
      new Node("pds inner", Node.Type.PHYSICAL_SOURCE),
      new Node("vds inner", Node.Type.VIRTUAL_SOURCE)
    );
  }

  @Test
  public void shouldReturnEmptyNodesListIfNamespaceThrowsError() throws NamespaceException {
    when(namespaceService.list(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenThrow(new RuntimeException("Inner Context doesn't exist"));

    when(namespaceService.list(new NamespaceKey(ImmutableList.of("path"))))
      .thenThrow(new RuntimeException("Outer Context doesn't exist"));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableList<Node> nodes = provider.getChildrenInScope(ImmutableList.of("path"));

    assertThat(nodes).isEmpty();
  }

  @Test
  public void shouldReturnAllColumnsForSourceIgnoringContext() {
    // NOTE: we use upper case here because of how MockDataTable is implemented :(
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("path"))))
      .thenReturn(tableWithColumns(
        Column.typedColumn("TIME", SqlTypeName.DOUBLE),
        Column.typedColumn("HAS", SqlTypeName.VARCHAR),
        Column.typedColumn("COME", SqlTypeName.INTEGER)
      ));

    // Let's create root here to make sure it is not used
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(tableWithColumns(
        Column.typedColumn("TO", SqlTypeName.DOUBLE),
        Column.typedColumn("MAKE", SqlTypeName.VARCHAR),
        Column.typedColumn("THINGS", SqlTypeName.INTEGER),
        Column.typedColumn("RIGHT", SqlTypeName.INTEGER)
      ));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableSet<Column> columns = provider.getColumnsByFullPath(ImmutableList.of("path"));

    assertThat(columns).containsExactlyInAnyOrder(
      Column.typedColumn("TIME", SqlTypeName.DOUBLE),
      Column.typedColumn("HAS", SqlTypeName.VARCHAR),
      Column.typedColumn("COME", SqlTypeName.INTEGER)
    );
  }

  @Test
  public void shouldReturnEmptySchemaIfCantFetchSource() {
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("path"))))
      .thenReturn(null);

    // Let's create root here to make sure it is not used
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(tableWithColumns(
        Column.typedColumn("ANOTHER", SqlTypeName.DOUBLE),
        Column.typedColumn("ONE", SqlTypeName.VARCHAR),
        Column.typedColumn("BITES", SqlTypeName.INTEGER),
        Column.typedColumn("THE", SqlTypeName.INTEGER),
        Column.typedColumn("DUST", SqlTypeName.INTEGER)
      ));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableSet<Column> columns = provider.getColumnsByFullPath(ImmutableList.of("path"));

    assertThat(columns).isEmpty();
  }

  @Test
  public void shouldReturnEmptySchemaIfFetchingSourceThrows() {
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("path"))))
      .thenThrow(new RuntimeException("BOOOM"));

    // Let's create root here to make sure it is not used
    when(entityExplorer.getTableNoResolve(new NamespaceKey(ImmutableList.of("root", "path"))))
      .thenReturn(tableWithColumns(Column.typedColumn("SAMPLE", SqlTypeName.DOUBLE)));

    AutocompleteSchemaProvider provider = createProvider(ImmutableList.of("root"));

    ImmutableSet<Column> columns = provider.getColumnsByFullPath(ImmutableList.of("path"));

    assertThat(columns).isEmpty();
  }

  private AutocompleteSchemaProvider createProvider(ImmutableList<String> context) {
    return new AutocompleteSchemaProvider(
      USER,
      namespaceService,
      connectionReader,
      entityExplorer,
      context
    );
  }

  private static DremioTable tableWithColumns(Column... columns) {
    NamespaceKey ignored = new NamespaceKey("dummy");
    return MockDremioTableFactory.createFromSchema(ignored, SqlTypeFactoryImpl.INSTANCE,
      Arrays.stream(columns)
        .map(c -> MockSchemas.ColumnSchema.create(c.getName(), c.getType()))
        .collect(ImmutableList.toImmutableList())
    );
  }

  private static NameSpaceContainer folderContainer(String name) {
    return new NameSpaceContainer()
      .setType(NameSpaceContainer.Type.FOLDER)
      .setFolder(new FolderConfig().setName(name));
  }

  private static NameSpaceContainer physicalDatasetContainer(String name) {
    return new NameSpaceContainer()
      .setType(NameSpaceContainer.Type.DATASET)
      .setDataset(new DatasetConfig()
        .setName(name)
        .setType(DatasetType.PHYSICAL_DATASET)
      );
  }

  private static NameSpaceContainer virtualDatasetContainer(String name) {
    return new NameSpaceContainer()
      .setType(NameSpaceContainer.Type.DATASET)
      .setDataset(new DatasetConfig()
        .setName(name)
        .setType(DatasetType.VIRTUAL_DATASET)
      );
  }

  private static NameSpaceContainer spaceContainer(String name) {
    return new NameSpaceContainer()
      .setType(NameSpaceContainer.Type.SPACE)
      .setSpace(space(name));
  }

  private static NameSpaceContainer sourceContainer(String name) {
    return new NameSpaceContainer()
      .setType(NameSpaceContainer.Type.SOURCE)
      .setSource(source(name));
  }

  private static SpaceConfig space(String name) {
    return new SpaceConfig().setName(name);
  }

  private static SourceConfig source(String name) {
    return new SourceConfig().setName(name);
  }

  private static SourceConfig sourceMatching(String name) {
    return Mockito.argThat(s -> s != null && name.equals(s.getName()));
  }


  private static <T extends ConnectionConf<T, P>, P extends StoragePlugin> ConnectionConf<T, P> internalConfig() {
    return connectionConfig(true);
  }

  private static <T extends ConnectionConf<T, P>, P extends StoragePlugin> ConnectionConf<T, P> externalConfig() {
    return connectionConfig(false);
  }

  private static <T extends ConnectionConf<T, P>, P extends StoragePlugin> ConnectionConf<T, P> connectionConfig(boolean isInternal) {
    return new ConnectionConf<T, P>() {
      @Override
      public P newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
        return null;
      }

      @Override
      public boolean isInternal() {
        return isInternal;
      }
    };
  }

}
