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
import java.util.Set;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.store.TableMetadata;
import com.dremio.service.Service;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.annotations.VisibleForTesting;

public interface StatisticsService extends Service, StatisticsAdministrationService {
  Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos();

  interface Histogram {
    double quantile(double q);

    long estimateCount(Object e);

    Set<Object>  getFrequentItems(long threshold);
    /**
     * For a filter condition, estimate the selectivity (matching rows/total rows) for this histogram.
     *
     * @return estimated selectivity or NULL if it could not be estimated for any reason
     */
    Double estimatedRangeSelectivity(final RexNode filter);

    Long estimatedPointSelectivity(final RexNode filter);

    boolean isTDigestSet();

    boolean isItemsSketchSet();
  }

  String requestStatistics(List<Field> fields, NamespaceKey key, Double samplingRate);

  List<String> deleteStatistics(List<String> fields, NamespaceKey key);

  boolean deleteRowCountStatistics(NamespaceKey key);

  @Override
  default void validate(NamespaceKey key) {
  }

  @VisibleForTesting
  boolean deleteStatistic(String column, NamespaceKey key) throws Exception;

  Long getNDV(String column, NamespaceKey key);

  Long getRowCount(NamespaceKey key);

  Long getNullCount(String column, NamespaceKey key);

  Histogram getHistogram(String column, TableMetadata tableMetaData);

  @VisibleForTesting
  Histogram getHistogram(String column, NamespaceKey key, SqlTypeName sqlTypeName);

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
    public String requestStatistics(List<Field> fields,  NamespaceKey key, Double samplingRate) {
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
    public Histogram getHistogram(String column, TableMetadata tableMetaData) {
      throw new UnsupportedOperationException("StatisticsService.getHistogram called on a non-coordinator node");
    }

    @Override
    public Histogram getHistogram(String column, NamespaceKey key, SqlTypeName sqlTypeName) {
      throw new UnsupportedOperationException("StatisticsService.getHistogram called on a non-coordinator node");
    }

    @Override
    public List<String> deleteStatistics(List<String> fields, NamespaceKey key) {
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


  // Only Used for Tests when no statistics are collected
  StatisticsService MOCK_STATISTICS_SERVICE = new StatisticsService() {
    @Override
    public void close() throws Exception {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos() {
        return null;
    }

    @Override
    public String requestStatistics(List<Field> fields,  NamespaceKey key, Double samplingRate) {
      return null;
    }

    @Override
    public Long getNDV(String column, NamespaceKey key) {
      return null;
    }

    @Override
    public Long getRowCount(NamespaceKey key) {
      return null;
    }

    @Override
    public Long getNullCount(String column, NamespaceKey key) {
      return null;
    }

    @Override
    public Histogram getHistogram(String column, TableMetadata tableMetaData) {
      return null;
    }

    @Override
    public Histogram getHistogram(String column, NamespaceKey key, SqlTypeName sqlTypeName) {
      return null;
    }

    @Override
    public List<String> deleteStatistics(List<String> fields, NamespaceKey key) {
      return null;
    }

    @Override
    public boolean deleteRowCountStatistics(NamespaceKey key) {
      return false;
    }

    @Override
    public void setNdv(String column, Long val, NamespaceKey key) {
      return;
    }

    @Override
    public void setRowCount(Long val, NamespaceKey key) {
      return ;
    }

    @Override
    public boolean deleteStatistic(String column, NamespaceKey key) {
      return false;
    }


  };
}
