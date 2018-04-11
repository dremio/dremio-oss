/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.ischema.tables;

import java.util.List;
import java.util.UUID;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

@SuppressWarnings({ "unchecked", "rawtypes" })
public enum InfoSchemaTable {
  CATALOGS(CatalogsTable.DEFINITION),
  SCHEMATA(SchemataTable.DEFINITION),
  TABLES(TablesTable.DEFINITION),
  VIEWS(ViewsTable.DEFINITION),
  COLUMNS(ColumnsTable.DEFINITION);

  private final BaseInfoSchemaTable<?> definition;
  private final NamespaceKey key;

  private InfoSchemaTable(BaseInfoSchemaTable<?> definition) {
    this.definition = definition;
    this.key = new NamespaceKey(ImmutableList.of("INFORMATION_SCHEMA", name()));
  }

  public <T> Iterable<T> asIterable(
      String catalogName,
      String username,
      DatasetListingService service,
      SearchQuery query
  ) {
    return (Iterable<T>) definition.asIterable(catalogName, username, service, query);
  }

  public RecordReader asReader(
      String catalogName,
      String username,
      DatasetListingService service,
      SearchQuery query,
      List<SchemaPath> columns
  ) {
    return new PojoRecordReader(definition.getRecordClass(),
        definition.asIterable(catalogName, username, service, query).iterator(),
        columns);
  }

  public BatchSchema getSchema() {
    RecordDataType dataType = new PojoDataType(definition.getRecordClass());
    RelDataType type = dataType.getRowType(new JavaTypeFactoryImpl());
    return BatchSchema.fromCalciteRowType(type);
  }

  public SourceTableDefinition asTableDefinition(final DatasetConfig oldDataset) {
    return new SourceTableDefinition() {

      @Override
      public NamespaceKey getName() {
        return key;
      }

      @Override
      public DatasetConfig getDataset() throws Exception {
        final DatasetConfig dataset;
        if(oldDataset == null) {
          dataset = new DatasetConfig()
           .setFullPathList(key.getPathComponents())
          .setId(new EntityId(UUID.randomUUID().toString()))
          .setType(DatasetType.PHYSICAL_DATASET);

        } else {
          dataset = oldDataset;
        }

        return dataset
            .setName(key.getName())
            .setReadDefinition(new ReadDefinition()
                .setScanStats(new ScanStats().setRecordCount(100l)
                    .setScanFactor(ScanCostFactor.OTHER.getFactor())))
            .setOwner(SystemUser.SYSTEM_USERNAME)
            .setPhysicalDataset(new PhysicalDataset())
            .setRecordSchema(getSchema().toByteString())
            .setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      }


      @Override
      public List<DatasetSplit> getSplits() throws Exception {
        // Always a single split.
        DatasetSplit split = new DatasetSplit();
        split.setSize(1l);
        split.setSplitKey("1");
        return ImmutableList.of(split);
      }

      @Override
      public boolean isSaveable() {
        return true;
      }

      @Override
      public DatasetType getType() {
        return DatasetType.PHYSICAL_DATASET;
      }};

  }
}
