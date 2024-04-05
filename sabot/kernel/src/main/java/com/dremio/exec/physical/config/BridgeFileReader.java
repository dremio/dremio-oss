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
package com.dremio.exec.physical.config;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class BridgeFileReader extends AbstractBase implements SubScan {
  private final BatchSchema schema;
  private final String bridgeSetId;

  @JsonCreator
  public BridgeFileReader(
      @JsonProperty("props") OpProps props,
      @JsonProperty("fullSchema") BatchSchema schema,
      @JsonProperty("bridgeSetId") String bridgeSetId) {
    super(props);
    this.schema = schema;
    this.bridgeSetId = bridgeSetId;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitBridgeFileReader(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    assert children.isEmpty();
    return new BridgeFileReader(props, schema, bridgeSetId);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.BRIDGE_FILE_READER.getNumber();
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @JsonIgnore
  @Override
  public List<List<String>> getReferencedTables() {
    return ImmutableList.of(Collections.singletonList("bridge"));
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  @Override
  public BatchSchema getFullSchema() {
    return schema;
  }

  public String getBridgeSetId() {
    return bridgeSetId;
  }

  @JsonIgnore
  @Override
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
  }
}
