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

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
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

  private final ElasticsearchScanSpec spec;
  private final List<DatasetSplit> splits;
  private final StoragePluginId pluginId;
  private final ByteString extendedProperty;

  @JsonCreator
  public ElasticsearchSubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("spec") ElasticsearchScanSpec spec,
      @JsonProperty("splits") List<DatasetSplit> splits,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("extendedProperty") ByteString extendedProperty){
    super(userName, schema, tableSchemaPath, columns);
    this.pluginId = pluginId;
    this.splits = splits;
    for (DatasetSplit split : this.splits) {
      if (split.getAffinitiesList() == null) {
        split.setAffinitiesList(new ArrayList<>());
      }
    }
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
    return new ElasticsearchSubScan(getUserName(), pluginId, spec, splits, getColumns(),
      Iterables.getOnlyElement(getReferencedTables()), getSchema(), extendedProperty);
  }

  public ElasticsearchScanSpec getSpec() {
    return spec;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.ELASTICSEARCH_SUB_SCAN_VALUE;
  }

}
