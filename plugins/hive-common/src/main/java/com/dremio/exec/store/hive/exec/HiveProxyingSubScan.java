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
package com.dremio.exec.store.hive.exec;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Class which wraps a Hive SubScan in a separate ClassLoader and exposes
 * it to Dremio.
 */
@JsonTypeName("hive-proxying-sub-scan")
@JsonDeserialize(using = HiveProxyingSubScanDeserializer.class)
public class HiveProxyingSubScan extends SubScanWithProjection {
  private final HiveProxiedSubScan proxiedSubScan;
  private final StoragePluginId pluginId;

  @JsonCreator
  public HiveProxyingSubScan(
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("wrappedHiveScan") HiveProxiedSubScan scan) {
    super(scan.getProps(), scan.getFullSchema(), scan.getTableSchemaPath(), scan.getColumns());
    this.pluginId  = pluginId;
    proxiedSubScan = scan;
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonProperty("wrappedHiveScan")
  public HiveProxiedSubScan getProxiedSubScan() {
    return proxiedSubScan;
  }

  @JsonIgnore
  public String getPluginName() {
    return pluginId.getName();
  }

  @JsonIgnore
  public ScanFilter getFilter() {
    return proxiedSubScan.getFilter();
  }

  @JsonIgnore
  public List<SplitAndPartitionInfo> getSplits() {
    return proxiedSubScan.getSplits();
  }

  @JsonIgnore
  public byte[] getExtendedProperty() {
    return proxiedSubScan.getExtendedProperty();
  }

  @JsonIgnore
  public List<String> getPartitionColumns() {
    return proxiedSubScan.getPartitionColumns();
  }

  @JsonIgnore
  @Override
  public int getOperatorType() {
    return proxiedSubScan.getOperatorType();
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) throws Exception {
    proxiedSubScan.collectMinorSpecificAttrs(writer);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    proxiedSubScan.populateMinorSpecificAttrs(reader);
  }

  @Override
  public boolean mayLearnSchema() {
    return proxiedSubScan.mayLearnSchema();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    HiveProxiedSubScan proxiedSubScan = this.proxiedSubScan.clone();
    return new HiveProxyingSubScan(pluginId, proxiedSubScan);
  }
}
