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
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpec;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpecList;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

// Class containing information for reading a single HBase region
@JsonTypeName("hbase-region-scan")
public class HBaseSubScan extends SubScanWithProjection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseSubScan.class);
  private static final String SCANS_ATTRIBUTE_KEY = "hbase-region-scan-scans";

  private final StoragePluginId pluginId;

  @JsonIgnore
  private List<HBaseSubScanSpec> scans;

  public HBaseSubScan(
    OpProps props,
    StoragePluginId pluginId,
    List<HBaseSubScanSpec> scans,
    List<SchemaPath> columns,
    BatchSchema fullSchema,
    List<String> tableSchemaPath
  ) {
    super(props, fullSchema, tableSchemaPath, columns);
    this.scans = scans;
    this.pluginId = pluginId;
  }

  @JsonCreator
  public HBaseSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("fullSchema") BatchSchema fullSchema,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath
      ) {
    super(props, fullSchema, tableSchemaPath, columns);
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
    return new HBaseSubScan(getProps(), getPluginId(), getScans(), getColumns(), getFullSchema(),
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

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    HBaseSubScanSpecList list = HBaseSubScanSpecList.newBuilder()
      .addAllScans(scans)
      .build();

    writer.writeProtoEntry(this, SCANS_ATTRIBUTE_KEY, list);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    ByteString buffer = reader.readProtoEntry(this, SCANS_ATTRIBUTE_KEY);
    this.scans = HBaseSubScanSpecList.parseFrom(buffer).getScansList();
  }
}
