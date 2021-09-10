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
package com.dremio.plugins.sysflight;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

/**
 * Flight group scan.
 */
public class SysFlightGroupScan extends AbstractBase implements GroupScan<SimpleCompleteWork>  {

  private final List<SchemaPath> columns;
  private final List<String> datasetPath;
  private final BatchSchema schema;
  private final StoragePluginId pluginId;

  public SysFlightGroupScan(OpProps props,
                            List<String> datasetPath,
                            BatchSchema schema,
                            List<SchemaPath> columns,
                            StoragePluginId pluginId
                            ) {
    super(props);
    this.columns = columns;
    this.datasetPath = datasetPath;
    this.schema = schema;
    this.pluginId = pluginId;
  }

  @Override
  @JsonIgnore
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  @JsonIgnore
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.NONE;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.FLIGHT_SUB_SCAN_VALUE;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof SysFlightGroupScan)) {
      return false;
    }
    SysFlightGroupScan castOther = (SysFlightGroupScan) other;
    return Objects.equal(datasetPath, castOther.datasetPath) && Objects.equal(schema, castOther.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetPath, schema);
  }

  @Override
  public String toString() {
    return new EntityPath(datasetPath).getName();
  }

  @Override
  public Iterator<SimpleCompleteWork> getSplits(ExecutionNodeMap executionNodes) {
    return Collections.<SimpleCompleteWork>singletonList(new SimpleCompleteWork(1)).iterator();
  }

  @Override
  public SubScan getSpecificScan(List<SimpleCompleteWork> work) {
    return new SysFlightSubScan(props, new EntityPath(datasetPath).getComponents(), schema, getColumns(), pluginId);
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
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
    return new SysFlightGroupScan(props, datasetPath, schema, columns, pluginId);
  }

}
