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
package com.dremio.exec.store.mfunctions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.google.common.base.Objects;

/**
 * A new scan for iceberg metadata functions
 */
final class MetadataFunctionsGroupScan extends AbstractBase implements GroupScan<SimpleCompleteWork> {

  private final MFunctionCatalogMetadata tableMetadata;
  private final String metadataLocation;
  private final List<SchemaPath> projectedColumns;
  private final BatchSchema schema;

  public MetadataFunctionsGroupScan(
    OpProps props,
    MFunctionCatalogMetadata tableMetadata,
    BatchSchema schema,
    List<SchemaPath> projectedColumns,
    String metadataLocation) {
    super(props);
    this.schema = schema;
    this.tableMetadata = tableMetadata;
    this.metadataLocation = metadataLocation;
    this.projectedColumns = projectedColumns;
  }


  @Override
  public SubScan getSpecificScan(List<SimpleCompleteWork> work) throws ExecutionSetupException {
    return new MetadataFunctionsSubScan(
      props,
      schema,
      tableMetadata.getMetadataFunctionName(),
      tableMetadata.getNamespaceKey().getPathComponents(),
      tableMetadata.getStoragePluginId(),
      projectedColumns,
      metadataLocation);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.ICEBERG_METADATA_FUNCTIONS_READER.getNumber();
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }


  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof MetadataFunctionsGroupScan)) {
      return false;
    }
    MetadataFunctionsGroupScan castOther = (MetadataFunctionsGroupScan) other;
    return Objects.equal(tableMetadata.getMetadataFunctionName(), castOther.tableMetadata.getMetadataFunctionName())
      && Objects.equal(tableMetadata.getUnderlyingTable(), castOther.tableMetadata.getUnderlyingTable())
      && Objects.equal(tableMetadata.getNamespaceKey(), castOther.tableMetadata.getNamespaceKey());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema);
  }


  @Deprecated
  public List<String> getTableSchemaPath() {
    return tableMetadata.getNamespaceKey().getPathComponents();
  }

  /**
   * Iceberg Metadata functions like history/snapshot/manifests doesn't need distribution.
   */
  @Override
  public Iterator<SimpleCompleteWork> getSplits(ExecutionNodeMap executionNodes) {
    return Collections.<SimpleCompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
  }

  @Override
  public List<SchemaPath> getColumns() {
    return Collections.emptyList();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitGroupScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return this;
  }


}
