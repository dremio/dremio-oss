/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning;

import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

/**
 * Elasticsearch sub-scan.
 */
@JsonTypeName("elasticsearch-sub-scan")
public class ElasticsearchSubScan extends SubScanWithProjection {

  private final ElasticsearchScanSpec spec;
  private final List<DatasetSplit> splits;
  private final StoragePluginId pluginId;
  private final ReadDefinition readDefinition;

  @JsonCreator
  public ElasticsearchSubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("spec") ElasticsearchScanSpec spec,
      @JsonProperty("readDefinition") ReadDefinition readDefinition,
      @JsonProperty("splits") List<DatasetSplit> splits,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath,
      @JsonProperty("schema") BatchSchema schema){
    super(userName, schema, tableSchemaPath, columns);
    this.pluginId = pluginId;
    this.readDefinition = readDefinition;
    this.splits = splits;
    this.spec = spec;
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
    return new ElasticsearchSubScan(getUserName(), pluginId, spec, readDefinition, splits, getColumns(), getTableSchemaPath(), getSchema());
  }

  public ReadDefinition getReadDefinition() {
    return readDefinition;
  }

  public ElasticsearchScanSpec getSpec() {
    return spec;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE;
  }

}
