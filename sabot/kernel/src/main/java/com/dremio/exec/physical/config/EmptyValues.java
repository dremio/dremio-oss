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
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class EmptyValues extends AbstractBase implements SubScan {

  private final BatchSchema schema;

  @JsonCreator
  public EmptyValues(
      @JsonProperty("props") OpProps props, @JsonProperty("fullSchema") BatchSchema schema) {
    super(props);
    this.schema = schema;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitEmptyValues(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    assert children.isEmpty();
    return new EmptyValues(props, schema);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.VALUES_READER.getNumber();
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @JsonIgnore
  @Override
  public List<List<String>> getReferencedTables() {
    return ImmutableList.of(Collections.singletonList("values"));
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  @Override
  public BatchSchema getFullSchema() {
    return schema;
  }

  @JsonIgnore
  @Override
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
  }
}
