/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.hbase;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

/**
 * HBase group scan.
 */
public class HBaseGroupScan extends AbstractGroupScan {

  private final HBaseScanSpec spec;
  private final long rowCountEstimate;

  public HBaseGroupScan(
      HBaseScanSpec spec,
      TableMetadata table,
      List<SchemaPath> columns,
      long rowCountEstimate
      ) {
    super(table, columns);
    this.spec = spec;
    this.rowCountEstimate = rowCountEstimate;
  }

  @JsonProperty("spec")
  public HBaseScanSpec getScanSpec() {
    return spec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    List<HBaseSubScanSpec> splitWork = FluentIterable.from(work).transform(new Function<SplitWork, HBaseSubScanSpec>(){
      @Override
      public HBaseSubScanSpec apply(SplitWork input) {
        return toSubScan(input.getSplit());
      }}).toList();

    return new HBaseSubScan(
        getDataset().getStoragePluginId(),
        getUserName(),
        splitWork,
        getColumns(),
        getSchema(),
        Iterables.getOnlyElement(getReferencedTables())
        );
  }

  private HBaseSubScanSpec toSubScan(DatasetSplit split) {
    KeyRange range = KeyRange.fromSplit(split).intersection(spec.getKeyRange());
    return new HBaseSubScanSpec(spec.getTableName().getNamespaceAsString(), spec.getTableName().getQualifierAsString(),
        range.getStart(), range.getStop(), spec.getSerializedFilter());
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HBASE_SUB_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof HBaseGroupScan)) {
      return false;
    }
    HBaseGroupScan castOther = (HBaseGroupScan) other;
    return Objects.equal(spec, castOther.spec) && Objects.equal(rowCountEstimate, castOther.rowCountEstimate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(spec, rowCountEstimate);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("spec", spec).add("rowCountEstimate", rowCountEstimate).toString();
  }

}
