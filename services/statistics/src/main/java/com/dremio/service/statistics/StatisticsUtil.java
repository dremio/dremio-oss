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
package com.dremio.service.statistics;

import static com.dremio.service.statistics.StatisticsServiceImpl.ROW_COUNT_IDENTIFIER;

import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.statistics.proto.StatisticId;

/**
 * Utility class for statistics
 */
public class StatisticsUtil {

  public static StatisticId createStatisticId(String column, String table) {
    return new StatisticId().setTablePath(table.toLowerCase()).setColumn(column.toLowerCase());
  }

  public static StatisticId createStatisticId(String column, NamespaceKey table) {
    return new StatisticId().setTablePath(table.toString().toLowerCase()).setColumn(column.toLowerCase());
  }

  public static StatisticId createRowCountStatisticId(String table) {
    return new StatisticId().setTablePath(table.toLowerCase()).setColumn(ROW_COUNT_IDENTIFIER);
  }

  public static StatisticId createRowCountStatisticId(NamespaceKey table) {
    return new StatisticId().setTablePath(table.toString().toLowerCase()).setColumn(ROW_COUNT_IDENTIFIER);
  }
}
