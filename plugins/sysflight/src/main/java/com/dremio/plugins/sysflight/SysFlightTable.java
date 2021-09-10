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
package com.dremio.plugins.sysflight;

import java.util.Iterator;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;

/**
 * SysFlightTable
 */
public class SysFlightTable implements DatasetHandle, DatasetMetadata, PartitionChunkListing {

  private static final long RECORD_COUNT = 10000L;
  private static final long SIZE_IN_BYTES = 100000L;

  private static final DatasetStats DATASET_STATS =
    DatasetStats.of(RECORD_COUNT, ScanCostFactor.OTHER.getFactor());
  private static final ImmutableList<PartitionChunk> PARTITION_CHUNKS =
    ImmutableList.of(PartitionChunk.of(DatasetSplit.of(SIZE_IN_BYTES, RECORD_COUNT)));

  private final FlightClient client;
  private final EntityPath entityPath;

  public SysFlightTable(EntityPath entityPath,
                        FlightClient client) {
    this.entityPath = entityPath;
    this.client = client;
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
    return new BatchSchema(client.getSchema(FlightDescriptor.path(entityPath.getName())).getSchema().getFields());
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return PARTITION_CHUNKS.iterator();
  }
}
