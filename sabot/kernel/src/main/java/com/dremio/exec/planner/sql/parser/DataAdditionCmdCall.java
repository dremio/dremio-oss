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

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.catalog.DremioTable;

/**
 * Data addition commands (CTAS, INSERT) implement this interface
 */
public interface DataAdditionCmdCall {

  /**
   * @return list partition column names
   */
  List<String> getPartitionColumns(DremioTable dremioTable);

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
  PartitionDistributionStrategy getPartitionDistributionStrategy();

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
}
