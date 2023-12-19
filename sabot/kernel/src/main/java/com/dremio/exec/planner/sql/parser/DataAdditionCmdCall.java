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
package com.dremio.exec.planner.sql.parser;

import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.planner.sql.PartitionTransform;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Data addition commands (CTAS, INSERT) implement this interface
 */
public interface DataAdditionCmdCall {

  /**
   * @return list partition column names
   */
  default List<String> getPartitionColumns(DremioTable dremioTable) {
    Preconditions.checkNotNull(dremioTable);
    List<String> columnNames =  dremioTable.getDatasetConfig().getReadDefinition().getPartitionColumnsList();
    return columnNames != null ? columnNames : Lists.newArrayList();
  }

  List<PartitionTransform> getPartitionTransforms(DremioTable dremioTable);

  /**
   * @return list of sort column names
   */
  List<String> getSortColumns();

  /**
   *
   * @return
   */
  List<String> getDistributionColumns();

  /**
   *
   * @return
   */
  default PartitionDistributionStrategy getPartitionDistributionStrategy(
    SqlHandlerConfig config, List<String> partitionFieldNames, Set<String> fieldNames) {
    PartitionDistributionStrategy partitionDistributionStrategy =
      PartitionDistributionStrategy.getPartitionDistributionStrategy(
        config.getContext().getOptions().getOption(ExecConstants.WRITER_PARTITION_DISTRIBUTION_MODE));

      // DX-50375: when we use VALUES clause in INSERT command, the field names end up with using expr, e.g., "EXPR%$0",
      // which are not the real underlying field names. Keep to use 'UNSPECIFIED' for this scenario.
      for (String partitionFieldName : partitionFieldNames) {
        if(!fieldNames.contains(partitionFieldName)) {
          return PartitionDistributionStrategy.UNSPECIFIED;
        }
      }

      return partitionDistributionStrategy;
  }

  /**
   *
   * @return
   */
  boolean isSingleWriter();

  /**
   *
   * @return
   */
  List<String> getFieldNames();

  /**
   *
   * @return Query part of CTAS or INSERT command
   */
  SqlNode getQuery();

  /**
   *
   * @return
   */
  default String getLocation() {
    return null;
  }
}
