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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.io.IOException;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.DatasetRetrievalOptions;

/**
 * Interface to be extended by plan builders for new metadata refresh flow.
 */
public interface MetadataRefreshPlanBuilder {

  /**
   * Return the root prel node after building the Prel
   */
  Prel buildPlan();

  /**
   * Returns a listing of partition chunks. There must be one or more partition chunks in a dataset.
   *
   * @return listing of partition chunk handles, not null
   * @param retrievalOptions
   */
  PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions retrievalOptions) throws ConnectorException;
  /**
   * Setup all the metadata like schema, partitions which are needed to build the plan.
   */
  void setupMetadataForPlanning(PartitionChunkListing partitionChunkListing, DatasetRetrievalOptions retrievalOptions) throws IOException;
}
