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
package com.dremio.exec.planner.sql;

import com.dremio.exec.catalog.MetadataStatsCollector;
import com.dremio.exec.planner.logical.ViewTable;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

public interface DremioToRelContext {
  final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioToRelContext.class);

  interface DremioQueryToRelContext extends ToRelContext {
    SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext();

    RelRoot expandView(ViewTable view);

    MetadataStatsCollector getMetadataStatsCollector();
  }

  interface DremioSerializationToRelContext extends ToRelContext {}

  static DremioSerializationToRelContext createSerializationContext(RelOptCluster relOptCluster) {
    return new DremioSerializationToRelContext() {
      @Override
      public RelOptCluster getCluster() {
        return relOptCluster;
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      @Override
      public RelRoot expandView(
          RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        throw new UnsupportedOperationException();
      }
    };
  }

  static DremioQueryToRelContext createQueryContext(
      SqlValidatorAndToRelContext.BuilderFactory supplierSqlValidatorAndToRelContextBuilder,
      RelOptCluster relOptCluster,
      ViewExpander viewExpander,
      MetadataStatsCollector metadataStatsCollector) {
    return new DremioQueryToRelContext() {

      @Override
      public SqlValidatorAndToRelContext.Builder getSqlValidatorAndToRelContext() {
        return supplierSqlValidatorAndToRelContextBuilder.builder();
      }

      @Override
      public RelOptCluster getCluster() {
        return relOptCluster;
      }

      @Override
      public List<RelHint> getTableHints() {
        return ImmutableList.of();
      }

      @Override
      public RelRoot expandView(ViewTable view) {
        return viewExpander.expandView(view);
      }

      @Override
      public RelRoot expandView(
          RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        throw new IllegalStateException("This expander should not be used.");
      }

      @Override
      public MetadataStatsCollector getMetadataStatsCollector() {
        return metadataStatsCollector;
      }
    };
  }
}
