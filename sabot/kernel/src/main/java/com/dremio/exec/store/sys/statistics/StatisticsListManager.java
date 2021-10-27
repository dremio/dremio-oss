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
package com.dremio.exec.store.sys.statistics;

import java.sql.Timestamp;

import com.dremio.exec.proto.StatisticsRPC;
import com.dremio.service.Service;

/**
 * Exposes the acceleration store to the execution engine
 */
public interface StatisticsListManager extends Service {

  Iterable<StatisticsInfo> getStatisticsInfos();

  class StatisticsInfo {
    public final String table;
    public final String column;
    public final Timestamp created_at;
    public final Long number_of_distinct_values;
    public final Long row_count;
    public final Long null_count;
    public final String quantiles;
    public final String heavy_hitters;

    public StatisticsInfo(String table, String column, Timestamp createdAt, Long ndv, Long rowCount, Long nullCount, String quantiles, String heavyHitters) {
      this.table = table;
      this.column = column;
      this.created_at = createdAt;
      this.number_of_distinct_values = ndv;
      this.row_count = rowCount;
      this.null_count = nullCount;
      this.quantiles = quantiles;
      this.heavy_hitters = heavyHitters;
    }

    public boolean isValid() {
      return number_of_distinct_values != null || null_count != null;
    }

    public static StatisticsInfo fromProto(StatisticsRPC.StatisticsInfo statisticsInfo) {

      return new StatisticsInfo(statisticsInfo.getTable(),
        statisticsInfo.getColumn(),
        new Timestamp(statisticsInfo.getCreatedAt()),
        statisticsInfo.getNumberOfDistinctValues() == -1 ? null : statisticsInfo.getNumberOfDistinctValues(),
        statisticsInfo.getRowCount() == -1 ? null : statisticsInfo.getRowCount(),
        statisticsInfo.getNullCount() == -1 ? null : statisticsInfo.getNullCount(),
        statisticsInfo.getQuantiles().equals("null") ? null : statisticsInfo.getQuantiles(),
        statisticsInfo.getHeavyHitters().equals("null") ? null :statisticsInfo.getHeavyHitters());
    }

    public StatisticsRPC.StatisticsInfo toProto() {
      StatisticsRPC.StatisticsInfo.Builder protoStatisticsInfo = StatisticsRPC.StatisticsInfo.newBuilder();
      protoStatisticsInfo.setTable(table);
      protoStatisticsInfo.setColumn(column);
      protoStatisticsInfo.setCreatedAt(created_at.getTime());
      protoStatisticsInfo.setNumberOfDistinctValues(number_of_distinct_values == null ? -1 : number_of_distinct_values);
      protoStatisticsInfo.setRowCount(row_count == null ? -1 : row_count);
      protoStatisticsInfo.setNullCount(null_count == null ? -1 : null_count);
      protoStatisticsInfo.setQuantiles(quantiles == null ? "null" : quantiles);
      protoStatisticsInfo.setHeavyHitters(heavy_hitters == null ? "null" : heavy_hitters);
      return protoStatisticsInfo.build();
    }
  }
}
