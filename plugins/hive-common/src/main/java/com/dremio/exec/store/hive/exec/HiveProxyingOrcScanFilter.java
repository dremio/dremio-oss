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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Class which wraps a Hive OrcScanFilter in a separate ClassLoader and exposes
 * it to Dremio.
 */
@JsonTypeName("hive-proxying-orc-scan-filter")
@JsonDeserialize(using = HiveProxyingOrcScanFilterDeserializer.class)
public class HiveProxyingOrcScanFilter implements ScanFilter {

  static final String JSON_PROP_PLUGINID = "pluginId";
  static final String JSON_PROP_WRAPPEDHIVEORCSCANFILTER = "wrappedHiveOrcScanFilter";
  static final String FILTER_COLUMNS = "filterColumns";

  private final HiveProxiedOrcScanFilter proxiedOrcScanFilter;
  private final StoragePluginId pluginId;

  @JsonCreator
  public HiveProxyingOrcScanFilter(
    @JsonProperty(JSON_PROP_PLUGINID) StoragePluginId pluginId,
    @JsonProperty(JSON_PROP_WRAPPEDHIVEORCSCANFILTER) HiveProxiedOrcScanFilter proxiedOrcScanFilter) {
    this.pluginId = pluginId;
    this.proxiedOrcScanFilter = proxiedOrcScanFilter;
  }

  @JsonProperty(JSON_PROP_PLUGINID)
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonProperty(JSON_PROP_WRAPPEDHIVEORCSCANFILTER)
  public HiveProxiedOrcScanFilter getProxiedOrcScanFilter() {
    return proxiedOrcScanFilter;
  }

  @Override
  public double getCostAdjustment() {
    return proxiedOrcScanFilter.getCostAdjustment();
  }

  @Override
  public String toString() {
    return proxiedOrcScanFilter.toString();
  }

  @Override
  public boolean equals(Object o) {
    return proxiedOrcScanFilter.equals(o);
  }

  @Override
  public int hashCode() {
    return proxiedOrcScanFilter.hashCode();
  }

  @Override
  @JsonProperty(FILTER_COLUMNS)
  public List<SchemaPath> getPaths() {
    return Collections.singletonList(proxiedOrcScanFilter.getColumn());
  }

  @Override
  @JsonIgnore
  public RexNode getRexFilter() {
    return getProxiedOrcScanFilter().getRexFilter();
  }

  @Override
  @JsonIgnore
  public RexNode getExactRexFilter() {
    // No Exact definition for OrcScanFilter
    return getProxiedOrcScanFilter().getRexFilter();
  }


}
