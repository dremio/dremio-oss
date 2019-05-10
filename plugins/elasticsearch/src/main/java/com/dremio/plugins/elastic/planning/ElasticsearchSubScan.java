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
package com.dremio.plugins.elastic.planning;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import io.protostuff.ByteString;

/**
 * Elasticsearch sub-scan.
 */
@JsonTypeName("elasticsearch-sub-scan")
public class ElasticsearchSubScan extends SubScanWithProjection {
  private static final String SPLITS_ATTRIBUTE_KEY = "elasticsearch-subscan-splits";

  private final ElasticsearchScanSpec spec;
  private final StoragePluginId pluginId;
  private final ByteString extendedProperty;

  @JsonIgnore
  private List<SplitInfo> splits;

  public ElasticsearchSubScan(
    OpProps props,
    StoragePluginId pluginId,
    ElasticsearchScanSpec spec,
    List<SplitInfo> splits,
    List<SchemaPath> columns,
    List<String> tableSchemaPath,
    BatchSchema fullSchema,
    ByteString extendedProperty){
    super(props, fullSchema, tableSchemaPath, columns);
    this.pluginId = pluginId;
    this.splits = splits;
    this.spec = spec;
    this.extendedProperty = extendedProperty;
  }

  @JsonCreator
  public ElasticsearchSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("spec") ElasticsearchScanSpec spec,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath,
      @JsonProperty("fullSchema") BatchSchema fullSchema,
      @JsonProperty("extendedProperty") ByteString extendedProperty) {
    super(props, fullSchema, tableSchemaPath, columns);
    this.pluginId = pluginId;
    this.spec = spec;
    this.extendedProperty = extendedProperty;
  }

  public StoragePluginId getPluginId(){
    return pluginId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new ElasticsearchSubScan(getProps(), pluginId, spec, splits, getColumns(),
      Iterables.getOnlyElement(getReferencedTables()), getFullSchema(), extendedProperty);
  }

  public ElasticsearchScanSpec getSpec() {
    return spec;
  }

  public List<SplitInfo> getSplits() {
    return splits;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    writer.writeSplits(this, SPLITS_ATTRIBUTE_KEY, splits);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    this.splits = reader.readSplits(this, SPLITS_ATTRIBUTE_KEY);
  }
}
