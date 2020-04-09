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
package com.dremio.exec.store.ischema.tables;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordDataType;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.pojo.PojoDataType;
import com.dremio.exec.store.pojo.PojoRecordReader;
import com.dremio.service.listing.DatasetListingService;
import com.google.common.collect.ImmutableList;

@SuppressWarnings({"unchecked", "rawtypes"})
public enum InfoSchemaTable implements DatasetHandle, DatasetMetadata, PartitionChunkListing {
  CATALOGS(CatalogsTable.DEFINITION),
  SCHEMATA(SchemataTable.DEFINITION),
  TABLES(TablesTable.DEFINITION),
  VIEWS(ViewsTable.DEFINITION),
  COLUMNS(ColumnsTable.DEFINITION);

  private static final long RECORD_COUNT = 100L;
  private static final long SIZE_IN_BYTES = 1000L;

  private static final DatasetStats DATASET_STATS = DatasetStats.of(RECORD_COUNT, ScanCostFactor.OTHER.getFactor());
  private static final ImmutableList<PartitionChunk> PARTITION_CHUNKS =
      ImmutableList.of(PartitionChunk.of(DatasetSplit.of(SIZE_IN_BYTES, RECORD_COUNT)));

  private final BaseInfoSchemaTable<?> definition;
  private final EntityPath entityPath;

  private InfoSchemaTable(BaseInfoSchemaTable<?> definition) {
    this.definition = definition;
    this.entityPath = new EntityPath(ImmutableList.of("INFORMATION_SCHEMA", name()));
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
      List<SchemaPath> columns,
      int batchSize
  ) {
    return new PojoRecordReader(definition.getRecordClass(),
        definition.asIterable(catalogName, username, service, query).iterator(),
        columns, batchSize);
  }

  @Override
  public EntityPath getDatasetPath() {
    return entityPath;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return DATASET_STATS;
  }

  @Override
  public BatchSchema getRecordSchema() {
    RecordDataType dataType = new PojoDataType(definition.getRecordClass());
    RelDataType type = dataType.getRowType(JavaTypeFactoryImpl.INSTANCE);
    return CalciteArrowHelper.fromCalciteRowType(type);
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return PARTITION_CHUNKS.iterator();
  }
}
