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

import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public interface StatisticsService extends Service, StatisticsAdministrationService {
  Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos();

  Logger logger = LoggerFactory.getLogger(StatisticsService.class);

  interface Histogram {
    double quantile(double q);

    /**
     * For a filter condition, estimate the selectivity (matching rows/total rows) for this histogram.
     *
     * @return estimated selectivity or NULL if it could not be estimated for any reason
     */
    Double estimatedRangeSelectivity(final RexNode filter);
  }

  String requestStatistics(List<String> columns, NamespaceKey key);

  List<String> deleteStatistics(List<String> columns, NamespaceKey key);

  boolean deleteRowCountStatistics(NamespaceKey key);

  @Override
  default void validate(NamespaceKey key) {
  }

  @VisibleForTesting
  boolean deleteStatistic(String column, NamespaceKey key) throws Exception;

  Long getNDV(String column, NamespaceKey key);

  Long getRowCount(NamespaceKey key);

  Long getNullCount(String column, NamespaceKey key);

  Histogram getHistogram(String column, NamespaceKey key);

  @VisibleForTesting
  void setNdv(String column, Long val, NamespaceKey key) throws Exception;

  @VisibleForTesting
  void setRowCount(Long val, NamespaceKey key) throws Exception;

  StatisticsService NO_OP = new StatisticsService() {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos() {
      throw new UnsupportedOperationException("StatisticsService.getStatisticsInfos called on a non-coordinator node");
    }

    @Override
    public String requestStatistics(List<String> columns, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.requestStatistics called on a non-coordinator node");
    }

    @Override
    public Long getNDV(String column, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.getNdv called on a non-coordinator node");
    }

    @Override
    public Long getRowCount(NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.getRowCount called on a non-coordinator node");
    }

    @Override
    public Long getNullCount(String column, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.getNullCount called on a non-coordinator node");
    }

    @Override
    public Histogram getHistogram(String column, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.getHistogram called on a non-coordinator node");
    }

    @Override
    public List<String> deleteStatistics(List<String> columns, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.deleteStatistics called on a non-coordinator node");
    }

    @Override
    public boolean deleteRowCountStatistics(NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.deleteRowCountStatistics called on a non-coordinator node");
    }

    @Override
    public void setNdv(String column, Long val, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.setNdv called on a non-coordinator node");
    }

    @Override
    public void setRowCount(Long val, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.setRowCount called on a non-coordinator node");
    }

    @Override
    public boolean deleteStatistic(String column, NamespaceKey key) {
      throw new UnsupportedOperationException("StatisticsService.setNdv called on a non-coordinator node");
    }


  };
}
