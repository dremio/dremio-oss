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
package com.dremio.exec.store.hive;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.hive.proxy.HiveProxiedOrcScanFilter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Implementation of {@link ScanFilter} for Hive-ORC (Optimized Row Columnar format files) scan
 */
public class ORCScanFilter extends HiveProxiedOrcScanFilter {

  private final SearchArgument sarg;
  private final StoragePluginId pluginId;

  @JsonCreator
  public ORCScanFilter(@JsonProperty("kryoBase64EncodedFilter") final String kryoBase64EncodedFilter,
                       @JsonProperty("pluginId") StoragePluginId pluginId,
                       @JsonProperty("column") SchemaPath column) {
    super(kryoBase64EncodedFilter, column);
    this.sarg = HiveUtilities.decodeSearchArgumentFromBase64(kryoBase64EncodedFilter);
    this.pluginId = pluginId;
  }

  public ORCScanFilter(final SearchArgument sarg, StoragePluginId pluginId, SchemaPath column) {
    super(HiveUtilities.encodeSearchArgumentAsBas64(sarg), column);
    Preconditions.checkNotNull(sarg, "expected a non-null filter expression");
    this.sarg = sarg;
    this.pluginId = pluginId;
  }

  @Override
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonIgnore
  public SearchArgument getSarg() {
    return sarg;
  }

  @JsonIgnore
  public double getCostAdjustment() {
    return ScanRelBase.DEFAULT_COST_ADJUSTMENT;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ORCScanFilter that = (ORCScanFilter) o;
    return Objects.equal(sarg, that.sarg);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(sarg);
  }

  @Override
  public String toString() {
    return "filterExpr = [" + sarg + "]";
  }
}
