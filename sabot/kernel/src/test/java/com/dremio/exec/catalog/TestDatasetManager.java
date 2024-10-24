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

import static com.dremio.exec.store.Views.isComplexType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.connector.impersonation.extensions.SupportsImpersonation;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.exec.catalog.CatalogImpl.IdentityResolver;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.AuthorizationContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.ImpersonationConf;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import javax.inject.Provider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Stubber;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieRuntimeException;

/** Tests for DatasetManager */
public class TestDatasetManager {

  private class CatalogIdentityResolver implements IdentityResolver {
    @Override
    public CatalogIdentity getOwner(List<String> path) throws NamespaceException {
      return null;
    }

    @Override
    public NamespaceIdentity toNamespaceIdentity(CatalogIdentity identity) {
      return null;
    }
  }

  @Test
  public void testAccessUsernameOverride() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);

    final MetadataStatsCollector statsCollector = mock(MetadataStatsCollector.class);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    when(metadataRequestOptions.getStatsCollector()).thenReturn(statsCollector);

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(Collections.singletonList("test"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);

    class FakeSource extends ConnectionConf<FakeSource, StoragePlugin>
        implements ImpersonationConf {
      @Override
      public StoragePlugin newPlugin(
          SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
        return null;
      }

      @Override
      public String getAccessUserName(String delegatedUser, String queryUserName) {
        return queryUserName;
      }
    }

    final FakeSource fakeSource = new FakeSource();

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    doReturn(fakeSource).when(managedStoragePlugin).getConnectionConf();
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(true);
    when(managedStoragePlugin.getDatasetMetadataState(eq(datasetConfig)))
        .thenReturn(DatasetMetadataState.builder().build());
    // newaccessuser should be used and not username
    doThrow(new RuntimeException("Wrong username"))
        .when(managedStoragePlugin)
        .checkAccess(namespaceKey, datasetConfig, "username", metadataRequestOptions);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            new CatalogIdentityResolver(),
            null,
            null,
            null);
    datasetManager.getTable(namespaceKey, metadataRequestOptions, false);
  }

  /** DX-16198 if doing a drop ignore the 800 line policy */
  @Test
  public void testIgnoreColumnCountOnDrop() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);

    final MetadataStatsCollector statsCollector = mock(MetadataStatsCollector.class);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    when(metadataRequestOptions.getStatsCollector()).thenReturn(statsCollector);

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(ImmutableList.of("test", "file", "foobar"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(false);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              assertEquals(
                  invocation.getArgument(2, DatasetRetrievalOptions.class).maxMetadataLeafColumns(),
                  Integer.MAX_VALUE);
              return Optional.empty();
            });

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            new CatalogIdentityResolver(),
            null,
            null,
            null);
    datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
  }

  /** DX-27465 */
  @Test
  public void testInlineViewUpdateWithComplexType() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final OptionManager optionManager = mock(OptionManager.class);

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);
    when(schemaConfig.getOptions()).thenReturn(optionManager);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    // Create a vds with ANY for complex type (old view)
    ViewFieldType type = new ViewFieldType();
    type.setType(SqlTypeName.ANY.toString());
    type.setName("struct_col");

    VirtualDataset virtualDataset = new VirtualDataset();
    virtualDataset.setSql("select * from test.file.foobar");
    virtualDataset.setSqlFieldsList(ImmutableList.of(type));

    // Set record schema and create dataset config for the view
    RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
    List<RelDataTypeField> fields = new ArrayList<>();
    RelDataTypeField field0 =
        new RelDataTypeFieldImpl("col1", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 =
        new RelDataTypeFieldImpl("col2", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);
    final RelDataType recordType = new RelRecordType(StructKind.FULLY_QUALIFIED, fields, true);
    RelDataTypeField structField = new RelDataTypeFieldImpl("struct_col", 0, recordType);
    final RelDataType rowType =
        new RelRecordType(StructKind.FULLY_QUALIFIED, ImmutableList.of(structField), true);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.VIRTUAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(ImmutableList.of("test", "file", "foobar"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);
    datasetConfig.setVirtualDataset(virtualDataset);
    datasetConfig.setRecordSchema(CalciteArrowHelper.fromCalciteRowType(rowType).toByteString());

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(false);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              assertEquals(
                  invocation.getArgument(2, DatasetRetrievalOptions.class).maxMetadataLeafColumns(),
                  Integer.MAX_VALUE);
              return Optional.empty();
            });

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    // get table and verify type and field information is properly updated from record schema
    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            new CatalogIdentityResolver(),
            null,
            null,
            null);
    DremioTable table = datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
    View.FieldType updatedField = ((ViewTable) table).getView().getFields().get(0);
    assertTrue(isComplexType(updatedField.getType()));
    assertEquals(
        updatedField.getField().toString(), "struct_col: Struct<col1: Int(32, true), col2: Utf8>");
  }

  @Test
  public void testImpersonationRequiresUser() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);

    class NonUserIdentity implements CatalogIdentity {
      @Override
      public String getName() {
        return "notauser";
      }
    }
    AuthorizationContext authContext = mock(AuthorizationContext.class);
    when(authContext.getSubject()).thenReturn(new NonUserIdentity());
    when(schemaConfig.getAuthContext()).thenReturn(authContext);

    final MetadataStatsCollector statsCollector = mock(MetadataStatsCollector.class);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    when(metadataRequestOptions.getStatsCollector()).thenReturn(statsCollector);

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(Collections.singletonList("test"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);

    StoragePlugin plugin =
        mock(StoragePlugin.class, withSettings().extraInterfaces(SupportsImpersonation.class));
    SupportsImpersonation supportsImpersonation = (SupportsImpersonation) plugin;
    when(supportsImpersonation.isImpersonationEnabled()).thenReturn(true);

    class FakeSource extends ConnectionConf<FakeSource, StoragePlugin>
        implements ImpersonationConf {
      @Override
      public StoragePlugin newPlugin(
          SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
        return plugin;
      }

      @Override
      public String getAccessUserName(String delegatedUser, String queryUserName) {
        return queryUserName;
      }
    }

    final FakeSource fakeSource = new FakeSource();

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getPlugin()).thenReturn(plugin);
    doReturn(fakeSource).when(managedStoragePlugin).getConnectionConf();
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(true);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            new CatalogIdentityResolver(),
            null,
            null,
            null);

    assertThatThrownBy(() -> datasetManager.getTable(namespaceKey, metadataRequestOptions, false))
        .isInstanceOf(UserException.class)
        .hasCauseInstanceOf(InvalidImpersonationTargetException.class);
  }

  @Test
  public void testCheckNeverPromoteWithNullConfig() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(false)
            .setNeverPromote(true)
            .build();

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);

    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("test"));
    when(sp.getDatasetHandle(any(), ArgumentMatchers.<GetDatasetOption[]>any()))
        .thenReturn(Optional.of(handle));

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(handle));
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(null);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);
    DremioTable table = datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
    assertThat(table).isNull();
  }

  @Test
  public void testCheckNeverPromoteWithShallowConfig() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(false)
            .setNeverPromote(true)
            .build();

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setId(new EntityId("test"));
    shallowDatasetConfig.setFullPathList(ImmutableList.of("test", "file", "foobar"));
    shallowDatasetConfig.setTotalNumSplits(0);

    ExtendedStoragePlugin sp = mock(ExtendedStoragePlugin.class);

    DatasetHandle handle = () -> new EntityPath(Lists.newArrayList("test"));
    when(sp.getDatasetHandle(any(), ArgumentMatchers.<GetDatasetOption[]>any()))
        .thenReturn(Optional.of(handle));

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(handle));
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);
    DremioTable table = datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
    assertThat(table).isNull();
  }

  /**
   * Validates metadata request option that is used to throw an exception when a table's version
   * context is resolved using the default source version mapping (instead of via AT syntax or via
   * session's source version mapping)
   *
   * <p>The main use case for this option is with the REFRESH REFLECTION job where we need to
   * validate that any cross Versioned catalog joins do not resolve using the default source version
   * mapping.
   *
   * @throws Exception
   */
  @Test
  public void testCheckErrorOnUnspecifiedSourceVersion() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    // Same options as what the planner would use for a REFRESH REFLECTION job
    final MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(true)
            .setNeverPromote(false)
            .setErrorOnUnspecifiedSourceVersion(true)
            .build();

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));

    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(sp.unwrap(VersionedPlugin.class)).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);
    try {
      datasetManager.getTable(sourceKey, metadataRequestOptions, true);
    } catch (UserException e) {
      assertThat(e.getMessage())
          .contains(
              "Version context for entity VersionedCatalog.\"Table\" must be specified using AT SQL syntax");
      return;
    }
    fail("getTable should have thrown exception");
  }

  @Test
  public void testCheckGetTableReturnsNullOnVersionException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);
    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);
    when(versionContextResolver.resolveVersionContext(sourceKey.toString(), versionContext))
        .thenThrow(new VersionNotFoundInNessieException() {});
    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);

    DremioTable returnedTable = datasetManager.getTable(sourceKey, metadataRequestOptions, true);
    assertThat(returnedTable).isNull();
  }

  @Test
  public void testCheckGetTableThrowsExceptionOnForbiddenException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);
    doThrow(new NessieForbiddenException(mock(NessieError.class)))
        .when(versionContextResolver)
        .resolveVersionContext(sourceKey.toString(), versionContext);
    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);

    assertThrows(
        NessieRuntimeException.class,
        () -> datasetManager.getTable(sourceKey, metadataRequestOptions, true));
  }

  @Test
  public void testCheckGetTableThrowsExceptionOnBadRequestException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);
    doThrow(new NessieBadRequestException(mock(NessieError.class)))
        .when(versionContextResolver)
        .resolveVersionContext(sourceKey.toString(), versionContext);
    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);

    assertThrows(
        RuntimeException.class,
        () -> datasetManager.getTable(sourceKey, metadataRequestOptions, true));
  }

  @Test
  public void testCheckGetTableThrowsExceptionOnRuntimeException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(sp.unwrap(VersionedPlugin.class)).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);
    doThrow(new RuntimeException())
        .when(versionContextResolver)
        .resolveVersionContext(sourceKey.toString(), versionContext);
    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);

    assertThrows(
        RuntimeException.class,
        () -> datasetManager.getTable(sourceKey, metadataRequestOptions, true));
  }

  @Test
  public void testCheckGetTableThrowsExceptionOnGenericException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);
    when(sp.unwrap(VersionedPlugin.class)).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(managedStoragePlugin.checkValidity(any(), eq(metadataRequestOptions))).thenReturn(false);
    doThrow(new IllegalStateException())
        .when(versionContextResolver)
        .resolveVersionContext(sourceKey.toString(), versionContext);
    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);

    assertThrows(
        IllegalStateException.class,
        () -> datasetManager.getTable(sourceKey, metadataRequestOptions, true));
  }

  @Test
  public void testCheckGetTableReturnsExceptionOnSourceException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    final VersionContextResolver versionContextResolver = mock(VersionContextResolver.class);
    final VersionedDatasetAdapterFactory versionedDatasetAdapterFactory =
        mock(VersionedDatasetAdapterFactory.class);
    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false))
        .thenThrow(UserException.validationError().message("Source Unavailable").buildSilently());

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            optionManager,
            "username",
            null,
            versionContextResolver,
            versionedDatasetAdapterFactory,
            null);
    try {
      DremioTable returnedTable = datasetManager.getTable(sourceKey, metadataRequestOptions, true);
      assertThat(returnedTable).isNull();
    } catch (UserException e) {
      assertThat(e.getMessage()).contains("Source Unavailable");
      return;
    }
    fail("getTable should have thrown exception");
  }

  @Test
  public void testCheckGetTableReturnsExceptionOnAccessException() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog", "Table"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(sourceKey))
        .thenThrow(new AccessControlException("Permission Denied"));
    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);
    try {
      DremioTable returnedTable = datasetManager.getTable(sourceKey, metadataRequestOptions, true);
      assertThat(returnedTable).isNull();
    } catch (AccessControlException e) {
      assertThat(e.getMessage()).contains("Permission Denied");
      return;
    }
    fail("getTable should have thrown exception");
  }

  @Test
  public void testGetTableWithNessieSourceAsKey() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("VersionedCatalog");
    final VersionContext versionContext = VersionContext.ofBranch("testBranch");
    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setFullPathList(ImmutableList.of("VersionedCatalog"));
    NamespaceKey tableKey = new NamespaceKey(shallowDatasetConfig.getFullPathList());
    when(metadataRequestOptions.getVersionForSource(sourceKey.getRoot(), tableKey))
        .thenReturn(versionContext);
    FakeVersionedPlugin sp = mock(FakeVersionedPlugin.class);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);
    when(managedStoragePlugin.getName()).thenReturn(sourceKey);
    when(sp.isWrapperFor(VersionedPlugin.class)).thenReturn(true);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(sourceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);

    DremioTable returnedTable = datasetManager.getTable(sourceKey, metadataRequestOptions, true);
    assertThat(returnedTable).isNull();
  }

  @Test
  public void testViewOwner() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    when(schemaConfig.getViewExpansionContext()).thenReturn(viewExpansionContext);

    final MetadataStatsCollector statsCollector = mock(MetadataStatsCollector.class);

    final MetadataRequestOptions metadataRequestOptions = mock(MetadataRequestOptions.class);
    when(metadataRequestOptions.getSchemaConfig()).thenReturn(schemaConfig);
    when(metadataRequestOptions.getStatsCollector()).thenReturn(statsCollector);

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig datasetConfig = new DatasetConfig();
    datasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    datasetConfig.setId(new EntityId("test"));
    datasetConfig.setFullPathList(ImmutableList.of("Arctic", "mytable"));
    datasetConfig.setReadDefinition(readDefinition);
    datasetConfig.setTotalNumSplits(0);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(false);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              assertEquals(
                  invocation.getArgument(2, DatasetRetrievalOptions.class).maxMetadataLeafColumns(),
                  Integer.MAX_VALUE);
              return Optional.empty();
            });

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final OptionManager optionManager = mock(OptionManager.class);

    DatasetConfig datasetConfig1 = mock(DatasetConfig.class);
    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig1);

    String datasetId = "";

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            null,
            "username",
            new CatalogIdentityResolver(),
            null,
            new VersionedDatasetAdapterFactory(),
            null);

    datasetManager.getTable(datasetId, metadataRequestOptions);
  }

  @Test
  public void testCheckPathTraversalNamespaceTableCannotBeAccessed() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");

    final MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(false)
            .setNeverPromote(true)
            .build();

    final ReadDefinition readDefinition = new ReadDefinition();
    readDefinition.setSplitVersion(0L);

    final DatasetConfig shallowDatasetConfig = new DatasetConfig();
    shallowDatasetConfig.setType(DatasetType.PHYSICAL_DATASET);
    shallowDatasetConfig.setId(new EntityId("test"));
    shallowDatasetConfig.setFullPathList(ImmutableList.of("test", "folder", "..", "file"));
    shallowDatasetConfig.setTotalNumSplits(0);

    final ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    final StoragePlugin sp = mock(FileSystemPlugin.class);
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(true);
    when(managedStoragePlugin.getPlugin()).thenReturn(sp);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(shallowDatasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever, namespaceService, optionManager, "username", null, null, null, null);
    assertThatThrownBy(() -> datasetManager.getTable(namespaceKey, metadataRequestOptions, true))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Not allowed to perform directory traversal");
  }

  /**
   * Test recovery from concurrent source modification. Number of exceptions thrown parameter
   * defines three tests: no concurrent modification, one concurrent modification (and successful
   * recovery), two concurrent modifications (and failure).
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  public void testCGetTable_sourceIsChangingRetry(int numExceptionsThrown) throws Exception {
    NamespaceKey namespaceKey = new NamespaceKey("test");

    // Metadata request options.
    SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(true)
            .build();

    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(ImmutableList.of("test", "folder", "file"))
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);

    // Storage plugin mock.
    ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    StoragePlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(false);
    when(managedStoragePlugin.getPlugin()).thenReturn(fileSystemPlugin);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.newBuilder().setMaxNestedLevel(1).build());
    when(managedStoragePlugin.getDatasetMetadataState(any()))
        .thenReturn(DatasetMetadataState.builder().setIsComplete(false).setIsExpired(true).build());
    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(datasetHandle));
    when(datasetHandle.getDatasetPath())
        .thenReturn(new EntityPath(datasetConfig.getFullPathList()));

    PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    // Namespace service mock.
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    // Mock throwing exceptions.
    DatasetSaver saver = mock(DatasetSaver.class);
    when(managedStoragePlugin.getSaver()).thenReturn(saver);
    Stubber stubber =
        numExceptionsThrown == 0
            ? doNothing()
            : doThrow(managedStoragePlugin.new StoragePluginChanging("first call"));
    if (numExceptionsThrown == 2) {
      stubber = stubber.doThrow(managedStoragePlugin.new StoragePluginChanging("second call"));
    } else {
      stubber = stubber.doNothing();
    }
    stubber.when(saver).save(any(), any(), any(), anyBoolean(), any(), anyString());

    DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            mock(OptionManager.class),
            "username",
            null,
            null,
            null,
            null);

    if (numExceptionsThrown <= 1) {
      // Verify that when one exception is thrown, the save call is retried.
      assertThat(datasetManager.getTable(namespaceKey, metadataRequestOptions, true)).isNotNull();
    } else {
      // Verify that when the second save() call fails, the exception is thrown from the getTable
      // method.
      ManagedStoragePlugin.StoragePluginChanging e =
          assertThrows(
              ManagedStoragePlugin.StoragePluginChanging.class,
              () -> datasetManager.getTable(namespaceKey, metadataRequestOptions, true));
      assertThat(e.getMessage()).isEqualTo("second call");
    }

    // Two calls were made when the first call threw.
    verify(saver, times(numExceptionsThrown == 0 ? 1 : 2))
        .save(any(), any(), any(), anyBoolean(), any(), anyString());
  }

  /**
   * Test recovery from concurrent modification with null read definition in {@link DatasetConfig}.
   * Number of exceptions thrown parameter defines three tests: no concurrent modification, one
   * concurrent modification (and successful recovery), two concurrent modifications (and failure).
   */
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  public void testCGetTable_nullReadDefinitionInCME(int numExceptionsThrown) throws Exception {
    NamespaceKey namespaceKey = new NamespaceKey("test");

    // Metadata request options.
    SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(true)
            .build();

    // Normal config.
    DatasetConfig datasetConfig =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(ImmutableList.of("test", "folder", "file"))
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);
    DatasetConfig datasetConfigWithNull =
        ProtostuffUtil.copy(datasetConfig).setReadDefinition(null);

    // Storage plugin mock.
    ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    StoragePlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(managedStoragePlugin.checkValidity(any(), any())).thenReturn(false);
    when(managedStoragePlugin.getPlugin()).thenReturn(fileSystemPlugin);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.newBuilder().setMaxNestedLevel(1).build());
    when(managedStoragePlugin.getDatasetMetadataState(any()))
        .thenReturn(DatasetMetadataState.builder().setIsComplete(false).setIsExpired(true).build());
    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(datasetHandle));
    when(datasetHandle.getDatasetPath())
        .thenReturn(new EntityPath(datasetConfig.getFullPathList()));

    PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    // Namespace service mock.
    NamespaceService namespaceService = mock(NamespaceService.class);
    if (numExceptionsThrown == 0) {
      when(namespaceService.getDataset(any())).thenReturn(datasetConfig);
    } else if (numExceptionsThrown == 1) {
      // Return null for first two calls, then return dataset with null read definition and finally
      // correct config.
      when(namespaceService.getDataset(any()))
          .thenReturn(null, null, datasetConfigWithNull, datasetConfig);
    } else {
      // Return null for first two calls and then return dataset with null read definition.
      when(namespaceService.getDataset(any())).thenReturn(null, null, datasetConfigWithNull);
    }

    // Mock throwing exceptions.
    DatasetSaver saver = mock(DatasetSaver.class);
    when(managedStoragePlugin.getSaver()).thenReturn(saver);
    Stubber stubber =
        numExceptionsThrown == 0
            ? doNothing()
            : doThrow(new ConcurrentModificationException("first call"));
    if (numExceptionsThrown == 2) {
      stubber = stubber.doThrow(new ConcurrentModificationException("second call"));
    } else {
      stubber = stubber.doNothing();
    }
    stubber.when(saver).save(any(), any(), any(), anyBoolean(), any(), anyString());

    DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            mock(OptionManager.class),
            "username",
            null,
            null,
            null,
            null);

    if (numExceptionsThrown <= 1) {
      // Verify that when one exception is thrown, the save call is retried.
      assertThat(datasetManager.getTable(namespaceKey, metadataRequestOptions, true)).isNotNull();
    } else {
      // Verify that when the second save() call fails, the exception is thrown from the
      // getTable method.
      ConcurrentModificationException e =
          assertThrows(
              ConcurrentModificationException.class,
              () -> datasetManager.getTable(namespaceKey, metadataRequestOptions, true));
      assertThat(e.getMessage()).isEqualTo("second call");
    }

    // Two calls were made when the first call threw.
    verify(saver, times(numExceptionsThrown == 0 ? 1 : 2))
        .save(any(), any(), any(), anyBoolean(), any(), anyString());
  }

  /**
   * Tests bulk table retrieval when validity checks return false. This exercises the code path that
   * invokes the metadata-io thread pool.
   */
  @Test
  public void testBulkGetTables_ValidityChecksReturnFalse() throws Exception {
    NamespaceKey namespaceKey1 = new NamespaceKey(ImmutableList.of("a", "b", "c"));
    NamespaceKey namespaceKey2 = new NamespaceKey(ImmutableList.of("a", "b", "d"));
    BulkRequest<NamespaceKey> req =
        BulkRequest.<NamespaceKey>builder().add(namespaceKey1).add(namespaceKey2).build();

    // Metadata request options.
    SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(true)
            .build();

    // Normal configs.
    DatasetConfig datasetConfig1 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(namespaceKey1.getPathComponents())
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);
    DatasetConfig datasetConfig2 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(namespaceKey2.getPathComponents())
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);

    // Storage plugin mock.
    ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    StoragePlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(managedStoragePlugin.getPlugin()).thenReturn(fileSystemPlugin);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.newBuilder().setMaxNestedLevel(1).build());
    when(managedStoragePlugin.getDatasetMetadataState(any()))
        .thenReturn(DatasetMetadataState.builder().setIsComplete(true).setIsExpired(false).build());
    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any()))
        .thenReturn(Optional.of(datasetHandle));
    when(datasetHandle.getDatasetPath())
        .thenReturn(new EntityPath(datasetConfig1.getFullPathList()))
        .thenReturn(new EntityPath(datasetConfig2.getFullPathList()));
    when(managedStoragePlugin.getSaver()).thenReturn(mock(DatasetSaver.class));

    // Validity checks should return false
    when(managedStoragePlugin.bulkCheckValidity(any(), any()))
        .thenAnswer(
            invocation -> {
              BulkRequest<NamespaceKey> keys = invocation.getArgument(0);
              return keys.handleRequests(Boolean.FALSE);
            });

    PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(eq(namespaceKey1.getRoot()), eq(false)))
        .thenReturn(managedStoragePlugin);
    when(pluginRetriever.getPlugin(eq(namespaceKey2.getRoot()), eq(false)))
        .thenReturn(managedStoragePlugin);

    // Namespace service mock.
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(eq(namespaceKey1))).thenReturn(datasetConfig1);
    when(namespaceService.getDataset(eq(namespaceKey2))).thenReturn(datasetConfig2);

    DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            mock(OptionManager.class),
            "username",
            null,
            null,
            null,
            MetadataIOPool.Factory.INSTANCE.newPool(0));

    BulkResponse<NamespaceKey, Optional<DremioTable>> res =
        datasetManager.bulkGetTables(req, metadataRequestOptions, true);
    Predicate<BulkResponse.Response<NamespaceKey, Optional<DremioTable>>> expected =
        response -> {
          NamespaceKey key = response.key();
          DremioTable dremioTable = response.response().toCompletableFuture().join().get();
          DatasetConfig datasetConfig = dremioTable.getDatasetConfig();
          return (key.equals(namespaceKey1) && datasetConfig.equals(datasetConfig1))
              || (key.equals(namespaceKey2) && datasetConfig.equals(datasetConfig2));
        };
    assertThat(res).matches(bulkResponse -> bulkResponse.responses().stream().allMatch(expected));
  }

  /**
   * Tests bulk table retrieval when checkValidity=false. This test verifies we don't execute on
   * metadata-io thread pool.
   */
  @Test
  public void testBulkGetTables_CheckValidityFalse() throws Exception {
    NamespaceKey namespaceKey1 = new NamespaceKey(ImmutableList.of("a", "b", "c"));
    NamespaceKey namespaceKey2 = new NamespaceKey(ImmutableList.of("a", "b", "d"));
    BulkRequest<NamespaceKey> req =
        BulkRequest.<NamespaceKey>builder().add(namespaceKey1).add(namespaceKey2).build();

    // Metadata request options.
    SchemaConfig schemaConfig = mock(SchemaConfig.class);
    when(schemaConfig.getUserName()).thenReturn("username");
    MetadataRequestOptions metadataRequestOptions =
        MetadataRequestOptions.newBuilder()
            .setSchemaConfig(schemaConfig)
            .setCheckValidity(false)
            .build();

    // Normal configs.
    DatasetConfig datasetConfig1 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(namespaceKey1.getPathComponents())
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);
    DatasetConfig datasetConfig2 =
        new DatasetConfig()
            .setType(DatasetType.PHYSICAL_DATASET)
            .setId(new EntityId("test"))
            .setFullPathList(namespaceKey2.getPathComponents())
            .setReadDefinition(new ReadDefinition().setSplitVersion(0L))
            .setTotalNumSplits(0);

    // Storage plugin mock.
    ManagedStoragePlugin managedStoragePlugin = mock(ManagedStoragePlugin.class);
    when(managedStoragePlugin.getId()).thenReturn(mock(StoragePluginId.class));
    StoragePlugin fileSystemPlugin = mock(FileSystemPlugin.class);
    when(managedStoragePlugin.getPlugin()).thenReturn(fileSystemPlugin);
    when(managedStoragePlugin.getDefaultRetrievalOptions())
        .thenReturn(DatasetRetrievalOptions.newBuilder().setMaxNestedLevel(1).build());
    when(managedStoragePlugin.getDatasetMetadataState(any()))
        .thenReturn(DatasetMetadataState.builder().setIsComplete(true).setIsExpired(false).build());

    // Simulate checkValidity=false. This means metadata always considered valid.
    when(managedStoragePlugin.bulkCheckValidity(any(), any()))
        .thenAnswer(
            invocation -> {
              BulkRequest<NamespaceKey> keys = invocation.getArgument(0);
              return keys.handleRequests(Boolean.TRUE);
            });

    PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(eq(namespaceKey1.getRoot()), eq(false)))
        .thenReturn(managedStoragePlugin);
    when(pluginRetriever.getPlugin(eq(namespaceKey2.getRoot()), eq(false)))
        .thenReturn(managedStoragePlugin);

    // Namespace service mock.
    NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(eq(namespaceKey1))).thenReturn(datasetConfig1);
    when(namespaceService.getDataset(eq(namespaceKey2))).thenReturn(datasetConfig2);

    MetadataIOPool metadataIOPool = mock(MetadataIOPool.class);

    DatasetManager datasetManager =
        new DatasetManager(
            pluginRetriever,
            namespaceService,
            mock(OptionManager.class),
            "username",
            null,
            null,
            null,
            metadataIOPool);

    // We shouldn't use the thread pool if checkValidity=false
    verify(metadataIOPool, never()).execute(any());

    BulkResponse<NamespaceKey, Optional<DremioTable>> res =
        datasetManager.bulkGetTables(req, metadataRequestOptions, true);
    Predicate<BulkResponse.Response<NamespaceKey, Optional<DremioTable>>> expected =
        response -> {
          NamespaceKey key = response.key();
          DremioTable dremioTable = response.response().toCompletableFuture().join().get();
          DatasetConfig datasetConfig = dremioTable.getDatasetConfig();
          return (key.equals(namespaceKey1) && datasetConfig.equals(datasetConfig1))
              || (key.equals(namespaceKey2) && datasetConfig.equals(datasetConfig2));
        };
    assertThat(res).matches(bulkResponse -> bulkResponse.responses().stream().allMatch(expected));
  }

  /** Fake Versioned Plugin interface for test */
  private interface FakeVersionedPlugin extends VersionedPlugin, StoragePlugin {}
}
