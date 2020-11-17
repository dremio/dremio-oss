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
package com.dremio.exec.store;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class NamespaceTable implements DremioTable {

  /**
   * In the future, we should move this to field extended metadata once we get to Arrow upstream has this.
   */
  public static final ImmutableSet<String> SYSTEM_COLUMNS = ImmutableSet.of(IncrementalUpdateUtils.UPDATE_COLUMN);

  private final TableMetadata dataset;
  private final boolean complexTypeSupport;

  public NamespaceTable(TableMetadata dataset, boolean complexTypeSupport) {
    this.dataset = Preconditions.checkNotNull(dataset);
    this.complexTypeSupport = complexTypeSupport;
  }

  @Override
  public ScanCrel toRel(ToRelContext toRelContext, RelOptTable relOptTable) {
    return new ScanCrel(toRelContext.getCluster(), toRelContext.getCluster().traitSetOf(Convention.NONE), dataset.getStoragePluginId(), dataset, null, 1.0d, true);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return CalciteArrowHelper.wrap(dataset.getSchema())
      .toCalciteRecordType(relDataTypeFactory, (Field f) -> !SYSTEM_COLUMNS.contains(f.getName()), complexTypeSupport);
  }

  public TableMetadata getDataset() {
    return dataset;
  }

  @Override
  public Statistic getStatistic() {
    return new StatisticImpl() {
      @Override
      public Double getRowCount() {
        return (double) dataset.getReadDefinition().getScanStats().getRecordCount();
      }

      @Override
      public List<RelReferentialConstraint> getReferentialConstraints() {
        return ImmutableList.of();
      }
    };
  }

  public boolean isApproximateStatsAllowed() {
    PhysicalDataset pd = dataset.getDatasetConfig().getPhysicalDataset();
    if(pd == null) {
      return false;
    }

    return pd.getAllowApproxStats() == null ? false : pd.getAllowApproxStats();
  }

  @Override
  public BatchSchema getSchema() {
    return dataset.getSchema();
  }

  @Override
  public DatasetConfig getDatasetConfig() {
    return dataset.getDatasetConfig();
  }

  @Override
  public TableType getJdbcTableType() {
    // ugly way to return correct table type for the system tables and information schema.
    if(dataset.getName().getRoot().equals("sys") || dataset.getName().getRoot().equals("INFORMATION_SCHEMA") ) {
      return TableType.SYSTEM_TABLE;
    }
    return TableType.TABLE;
  }

  public StoragePluginId getStoragePluginId() {
    return dataset.getStoragePluginId();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof NamespaceTable)) {
      return false;
    }
    NamespaceTable castOther = (NamespaceTable) other;
    return Objects.equal(dataset, castOther.dataset);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataset);
  }

  public abstract static class StatisticImpl implements Statistic {

    @Override
    public boolean isKey(ImmutableBitSet columns) {
      return false;
    }

    @Override
    public RelDistribution getDistribution() {
      return RelDistributionTraitDef.INSTANCE.getDefault();
    }

    @Override
    public List<RelCollation> getCollations() {
      return ImmutableList.of();
    }
  }

  @Override
  public NamespaceKey getPath() {
    return dataset.getName();
  }

  @Override
  public String getVersion() {
    return dataset.getVersion();
  }

}
