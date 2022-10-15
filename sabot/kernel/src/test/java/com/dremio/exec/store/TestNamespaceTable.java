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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Provider;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMetadataImpl;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.namespace.PartitionChunkMetadataImpl;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.UserExceptionAssert;

import io.protostuff.ByteString;

public class TestNamespaceTable {

  private static final RelDataTypeFactory typeFactory = JavaTypeFactoryImpl.INSTANCE;
  private static StoragePluginId storagePluginId;
  private static PartitionChunkMetadataImpl partitionChunkMetadata;
  private static String tableName;

  private static final class TestConnectionConf extends ConnectionConf<TestConnectionConf, StoragePlugin> {
    @Override
    public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return null;
    }
  }

  @BeforeClass
  public static void setup() {
    storagePluginId = new StoragePluginId(
      new SourceConfig().setConfig(ByteString.copyFrom(new byte[0])),
      new TestConnectionConf(),
      new SourceCapabilities());
    partitionChunkMetadata = new PartitionChunkMetadataImpl(
      PartitionProtobuf.PartitionChunk.newBuilder().setSplitCount(1).build(),
      null,
      () -> {
      },
      () -> null);
    tableName = "tbl";
  }

  @Test
  public void testExtendColumn() {
    NamespaceTable namespaceTable = createTableWithRandomColumName(typeFactory.createSqlType(SqlTypeName.BIGINT));
    Assert.assertEquals(1, namespaceTable.getSchema().getFields().size());

    DremioTable extendedTable = (DremioTable) namespaceTable.extend(
      createFieldWithRandomName(typeFactory.createSqlType(SqlTypeName.VARCHAR)));
    Assert.assertTrue(extendedTable instanceof NamespaceTable);
    NamespaceTable extendedNamespaceTable = (NamespaceTable) extendedTable;
    Assert.assertEquals(1, namespaceTable.getSchema().getFields().size());
    Assert.assertEquals(2, extendedNamespaceTable.getSchema().getFields().size());
    Assert.assertEquals(1, namespaceTable.getExtendedColumnOffset());
    Assert.assertEquals(1, extendedNamespaceTable.getExtendedColumnOffset());

  }

  @Test
  public void testExtendColumnWithSameName() {
    String columnName = "column";
    NamespaceTable namespaceTable = createTable(columnName, typeFactory.createSqlType(SqlTypeName.BIGINT));
    Assert.assertEquals(1, namespaceTable.getSchema().getFields().size());

    UserExceptionAssert.assertThatThrownBy(() -> namespaceTable.extend(
      createField(columnName, typeFactory.createSqlType(SqlTypeName.VARCHAR)))).hasMessageContaining("");
  }

  @Test
  public void testExtendColumns() {
    NamespaceTable namespaceTable = createTableWithRandomColumnNames(
      typeFactory.createSqlType(SqlTypeName.BIGINT),
      typeFactory.createSqlType(SqlTypeName.VARCHAR));
    Assert.assertEquals(2, namespaceTable.getSchema().getFields().size());

    DremioTable extendedTable = (DremioTable) namespaceTable.extend(createFieldsWithRandomNames(
      typeFactory.createSqlType(SqlTypeName.VARCHAR),
      typeFactory.createSqlType(SqlTypeName.VARCHAR),
      typeFactory.createSqlType(SqlTypeName.BIGINT)));
    Assert.assertTrue(extendedTable instanceof NamespaceTable);
    NamespaceTable extendedNamespaceTable = (NamespaceTable) extendedTable;
    Assert.assertEquals(2, namespaceTable.getSchema().getFields().size());
    Assert.assertEquals(5, extendedNamespaceTable.getSchema().getFields().size());
    Assert.assertEquals(2, namespaceTable.getExtendedColumnOffset());
    Assert.assertEquals(2, extendedNamespaceTable.getExtendedColumnOffset());

  }

  private NamespaceTable createTable(List<String> names, List<RelDataType> types) {
    return new NamespaceTable(new TableMetadataImpl(
      storagePluginId,
      new DatasetConfig()
        .setId(new EntityId(tableName))
        .setName(tableName)
        .setFullPathList(Collections.singletonList(tableName))
        .setType(DatasetType.PHYSICAL_DATASET)
        .setRecordSchema(CalciteArrowHelper.fromCalciteRowType(typeFactory.createStructType(createFields(names, types))).toByteString())
        .setReadDefinition(new ReadDefinition()
          .setScanStats(new ScanStats()
            .setRecordCount(123L)
            .setScanFactor(1.0))),
      "",
      MaterializedSplitsPointer.of(-1, Arrays.asList(partitionChunkMetadata), 1), null), false);
  }

  private NamespaceTable createTable(String name, RelDataType type) {
    return createTable(name != null ? Collections.singletonList(name) : Collections.emptyList(), Collections.singletonList(type));
  }

  private NamespaceTable createTableWithRandomColumnNames(RelDataType ...types) {
    return createTable(Collections.emptyList(), Arrays.asList(types));
  }

  private NamespaceTable createTableWithRandomColumName(RelDataType type) {
    return createTable(null, type);
  }

  private List<RelDataTypeField> createFields(List<String> names, List<RelDataType> types) {
    List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < types.size(); i++) {
      fields.add(new RelDataTypeFieldImpl(
        names.isEmpty() ? java.util.UUID.randomUUID().toString() : names.get(i),
        0,
        types.get(i)));
    }
    return fields;
  }

  private List<RelDataTypeField> createField(String name, RelDataType type) {
    List<RelDataTypeField> fields = new ArrayList<>();
    fields.add(new RelDataTypeFieldImpl(
      name == null ? java.util.UUID.randomUUID().toString() : name,
      0,
      type));
    return fields;
  }

  private List<RelDataTypeField> createFieldsWithRandomNames(RelDataType ...types) {
    return createFields(Collections.emptyList(), Arrays.asList(types));
  }

  private List<RelDataTypeField> createFieldWithRandomName(RelDataType type) {
    return createField(null, type);
  }
}
