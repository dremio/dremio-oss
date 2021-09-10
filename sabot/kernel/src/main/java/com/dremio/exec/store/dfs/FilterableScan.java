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
package com.dremio.exec.store.dfs;

import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;

/**
 * Methods required for filter pushdown
 */
public interface FilterableScan extends RelNode {
  ScanFilter getFilter();
  PruneFilterCondition getPartitionFilter();
  FilterableScan applyFilter(ScanFilter scanFilter);
  FilterableScan applyPartitionFilter(PruneFilterCondition partitionFilter);
  FilterableScan cloneWithProject(List<SchemaPath> projection, boolean preserveFilterColumns);
  TableMetadata getTableMetadata();
  BatchSchema getBatchSchema();
}
