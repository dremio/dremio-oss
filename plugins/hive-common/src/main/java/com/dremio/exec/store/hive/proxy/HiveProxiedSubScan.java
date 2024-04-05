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
package com.dremio.exec.store.hive.proxy;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.pf4j.ExtensionPoint;

/** Abstract class representing a Hive SubScan that is in a separate ClassLoader from Dremio's. */
public abstract class HiveProxiedSubScan implements ExtensionPoint {

  private final OpProps props;

  private final BatchSchema fullSchema;

  private final List<String> tablePath;

  private final List<SchemaPath> columns;

  protected HiveProxiedSubScan(
      OpProps props, BatchSchema fullSchema, List<String> tablePath, List<SchemaPath> columns) {
    this.props = props;
    this.fullSchema = fullSchema;
    this.tablePath = tablePath;
    this.columns = columns;
  }

  @JsonProperty("props")
  public OpProps getProps() {
    return props;
  }

  @JsonProperty("fullSchema")
  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  @JsonProperty("tableSchemaPath")
  public List<String> getTableSchemaPath() {
    return tablePath;
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  public abstract StoragePluginId getPluginId();

  public abstract ScanFilter getFilter();

  public abstract List<SplitAndPartitionInfo> getSplits();

  public abstract byte[] getExtendedProperty();

  public abstract List<String> getPartitionColumns();

  public abstract int getOperatorType();

  public abstract void collectMinorSpecificAttrs(MinorDataWriter writer);

  public abstract void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception;

  public abstract boolean mayLearnSchema();

  @Override
  public abstract HiveProxiedSubScan clone();
}
