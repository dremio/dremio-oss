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
package com.dremio.exec.planner.sql;

import com.dremio.BaseTestQuery;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsAlteringDatasetMetadata;
import com.dremio.connector.metadata.options.AlterMetadataOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;
import io.protostuff.Tag;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Provider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAlterTableChangeColumnSetOptionHandler extends BaseTestQuery {

  @SourceType(value = "column_option", label = "ColumnOptionTest", configurable = false)
  public static class ColumnOptionTestConf
      extends ConnectionConf<ColumnOptionTestConf, ColumnOptionTestPlugin> {
    @Tag(1)
    public boolean shouldChangeMetadata = false;

    @Override
    public ColumnOptionTestPlugin newPlugin(
        SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return new ColumnOptionTestPlugin(name, shouldChangeMetadata);
    }
  }

  public static final class ColumnOptionTestPlugin
      implements StoragePlugin, SupportsAlteringDatasetMetadata {
    private final DatasetHandle firstHandle;
    private final DatasetHandle secondHandle;
    private final DatasetMetadata FIRST_METADATA =
        DatasetMetadata.of(
            DatasetStats.of(10, 99.0),
            BatchSchema.of(Field.nullable("foo", ArrowType.Bool.INSTANCE)));
    private final DatasetMetadata SECOND_METADATA =
        DatasetMetadata.of(
            DatasetStats.of(10, 99.0),
            BatchSchema.of(Field.nullable("foo", ArrowType.Utf8.INSTANCE)));

    private final boolean shouldSetOptionChangeData;
    private boolean firstAccessOfTable = true;

    ColumnOptionTestPlugin(String name, boolean shouldSetOptionChangeData) {
      firstHandle = () -> new EntityPath(Arrays.asList(name, "first"));
      secondHandle = () -> new EntityPath(Arrays.asList(name, "first"));
      this.shouldSetOptionChangeData = shouldSetOptionChangeData;
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(
        EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
      if (firstAccessOfTable || !shouldSetOptionChangeData) {
        return Optional.of(firstHandle);
      }
      return Optional.of(secondHandle);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(
        DatasetHandle datasetHandle, ListPartitionChunkOption... options)
        throws ConnectorException {
      return new PartitionChunkListingImpl();
    }

    @Override
    public DatasetMetadata getDatasetMetadata(
        DatasetHandle datasetHandle,
        PartitionChunkListing chunkListing,
        GetMetadataOption... options)
        throws ConnectorException {
      if (firstAccessOfTable) {
        firstAccessOfTable = false;
        return FIRST_METADATA;
      }

      if (!shouldSetOptionChangeData) {
        return FIRST_METADATA;
      }
      return SECOND_METADATA;
    }

    @Override
    public boolean containerExists(EntityPath containerPath, GetMetadataOption... options) {
      return true;
    }

    @Override
    public DatasetMetadata alterMetadata(
        DatasetHandle datasetHandle,
        DatasetMetadata metadata,
        Map<String, AttributeValue> attributes,
        AlterMetadataOption... options)
        throws ConnectorException {
      return null;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
    }

    @Override
    public SourceState getState() {
      return SourceState.GOOD;
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
      return SourceCapabilities.NONE;
    }

    @Override
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      return null;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return StoragePluginRulesFactory.NoOpPluginRulesFactory.class;
    }

    @Override
    public void start() throws IOException {}

    @Override
    public void close() throws Exception {}

    @Override
    public DatasetMetadata alterDatasetSetColumnOption(
        DatasetHandle datasetHandle,
        DatasetMetadata metadata,
        String columnName,
        String attributeName,
        AttributeValue attributeValue,
        AlterMetadataOption... options)
        throws ConnectorException {
      if (shouldSetOptionChangeData) {
        return SECOND_METADATA;
      }

      return metadata;
    }
  }

  @BeforeClass
  public static void addPlugin() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();

    SourceConfig conf1 = new SourceConfig();
    conf1.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf1.setName("col_conf");
    final ColumnOptionTestConf colConf1 = new ColumnOptionTestConf();
    colConf1.shouldChangeMetadata = true;
    conf1.setConnectionConf(colConf1);
    getCatalogService().getSystemUserCatalog().createSource(conf1);

    SourceConfig conf2 = new SourceConfig();
    conf2.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf2.setName("col_conf2");
    final ColumnOptionTestConf colConf2 = new ColumnOptionTestConf();
    colConf2.shouldChangeMetadata = false;
    conf2.setConnectionConf(colConf2);
    getCatalogService().getSystemUserCatalog().createSource(conf2);
  }

  @Test
  public void testColumnOptionKeepsSameMetadata() throws Exception {
    testBuilder()
        .sqlQuery("ALTER TABLE \"col_conf\".\"first\" CHANGE COLUMN foo SET dummy_option = true")
        .ordered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            Boolean.TRUE,
            String.format("Table [%s] column [%s] options updated", "col_conf.first", "foo"))
        .go();
  }

  @Test
  public void testColumnOptionChangesMetadata() throws Exception {
    testBuilder()
        .sqlQuery("ALTER TABLE \"col_conf2\".\"first\" CHANGE COLUMN foo SET dummy_option = true")
        .ordered()
        .baselineColumns("ok", "summary")
        .baselineValues(
            Boolean.TRUE,
            String.format(
                "Table [%s] column [%s] options did not change", "col_conf2.first", "foo"))
        .go();
  }
}
