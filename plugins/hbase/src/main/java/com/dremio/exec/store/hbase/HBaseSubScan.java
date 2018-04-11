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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

// Class containing information for reading a single HBase region
@JsonTypeName("hbase-region-scan")
public class HBaseSubScan extends SubScanWithProjection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseSubScan.class);

  private final StoragePluginId pluginId;
  private final List<HBaseSubScanSpec> scans;

  @JsonCreator
  public HBaseSubScan(
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("userName") String userName,
      @JsonProperty("scans") List<HBaseSubScanSpec> scans,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath
      ) {
    super(userName, schema, tableSchemaPath, columns);
    this.scans = scans;
    this.pluginId = pluginId;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public List<HBaseSubScanSpec> getScans() {
    return scans;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new HBaseSubScan(getPluginId(), getUserName(), getScans(), getColumns(), getSchema(),
      Iterables.getOnlyElement(getReferencedTables()));
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HBASE_SUB_SCAN_VALUE;
  }

}
