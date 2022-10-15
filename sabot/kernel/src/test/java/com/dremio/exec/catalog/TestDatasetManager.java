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

import static com.dremio.exec.planner.physical.PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT;
import static com.dremio.exec.store.Views.isComplexType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.impersonation.extensions.SupportsImpersonation;
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
import com.dremio.exec.store.dfs.ImpersonationConf;
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

/**
 * Tests for DatasetManager
 */
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

    class FakeSource extends ConnectionConf<FakeSource, StoragePlugin> implements ImpersonationConf {
      @Override
      public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
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
    when(managedStoragePlugin.isCompleteAndValid(any(), any(), any())).thenReturn(true);
    // newaccessuser should be used and not username
    doThrow(new RuntimeException("Wrong username"))
      .when(managedStoragePlugin).checkAccess(namespaceKey, datasetConfig, "username", metadataRequestOptions);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager = new DatasetManager(pluginRetriever, namespaceService, optionManager, "username",
        new CatalogIdentityResolver(), null);
    datasetManager.getTable(namespaceKey, metadataRequestOptions, false);
  }

  /**
   * DX-16198 if doing a drop ignore the 800 line policy
   */
  @Test
  public void ignoreColumnCountOnDrop() throws Exception {
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
    when(managedStoragePlugin.isCompleteAndValid(any(), any(), any())).thenReturn(false);
    when(managedStoragePlugin.getDefaultRetrievalOptions()).thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any())).thenAnswer(invocation -> {
      Assert.assertEquals(invocation.getArgument(2, DatasetRetrievalOptions.class).maxMetadataLeafColumns(), Integer.MAX_VALUE);
      return Optional.empty();
    });

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager = new DatasetManager(pluginRetriever, namespaceService, optionManager, "username",
        new CatalogIdentityResolver(), null);
    datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
  }

  /**
   * DX-27465
   */
  @Test
  public void testInlineViewUpdateWithComplexType() throws Exception {
    final NamespaceKey namespaceKey = new NamespaceKey("test");

    final ViewExpansionContext viewExpansionContext = mock(ViewExpansionContext.class);
    when(viewExpansionContext.getQueryUser()).thenReturn(CatalogUser.from("newaccessuser"));

    final OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(FULL_NESTED_SCHEMA_SUPPORT)).thenReturn(true);

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
    RelDataTypeField field0 = new RelDataTypeFieldImpl(
      "col1", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataTypeField field1 = new RelDataTypeFieldImpl(
      "col2", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR));
    fields.add(field0);
    fields.add(field1);
    final RelDataType recordType = new RelRecordType(StructKind.FULLY_QUALIFIED, fields, true);
    RelDataTypeField structField = new RelDataTypeFieldImpl(
      "struct_col", 0, recordType);
    final RelDataType rowType = new RelRecordType(StructKind.FULLY_QUALIFIED, ImmutableList.of(structField), true);

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
    when(managedStoragePlugin.isCompleteAndValid(any(), any(), any())).thenReturn(false);
    when(managedStoragePlugin.getDefaultRetrievalOptions()).thenReturn(DatasetRetrievalOptions.DEFAULT);
    when(managedStoragePlugin.getDatasetHandle(any(), any(), any())).thenAnswer(invocation -> {
      Assert.assertEquals(invocation.getArgument(2, DatasetRetrievalOptions.class).maxMetadataLeafColumns(), Integer.MAX_VALUE);
      return Optional.empty();
    });

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    // get table and verify type and field information is properly updated from record schema
    final DatasetManager datasetManager = new DatasetManager(pluginRetriever, namespaceService, optionManager, "username",
        new CatalogIdentityResolver(), null);
    DremioTable table = datasetManager.getTable(namespaceKey, metadataRequestOptions, true);
    View.FieldType updatedField = ((ViewTable) table).getView().getFields().get(0);
    Assert.assertTrue(isComplexType(updatedField.getType()));
    Assert.assertEquals(updatedField.getField().toString(), "struct_col: Struct<col1: Int(32, true), col2: Utf8>");
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

    StoragePlugin plugin = mock(StoragePlugin.class, withSettings().extraInterfaces(SupportsImpersonation.class));
    SupportsImpersonation supportsImpersonation = (SupportsImpersonation) plugin;
    when(supportsImpersonation.isImpersonationEnabled()).thenReturn(true);

    class FakeSource extends ConnectionConf<FakeSource, StoragePlugin> implements ImpersonationConf {
      @Override
      public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
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
    when(managedStoragePlugin.isCompleteAndValid(any(), any(), any())).thenReturn(true);

    final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
    when(pluginRetriever.getPlugin(namespaceKey.getRoot(), false)).thenReturn(managedStoragePlugin);

    final NamespaceService namespaceService = mock(NamespaceService.class);
    when(namespaceService.getDataset(namespaceKey)).thenReturn(datasetConfig);

    final OptionManager optionManager = mock(OptionManager.class);

    final DatasetManager datasetManager = new DatasetManager(pluginRetriever, namespaceService, optionManager, "username",
        new CatalogIdentityResolver(), null);

    assertThatThrownBy(() -> datasetManager.getTable(namespaceKey, metadataRequestOptions, false))
      .isInstanceOf(UserException.class)
      .hasCauseInstanceOf(InvalidImpersonationTargetException.class);
  }
}
