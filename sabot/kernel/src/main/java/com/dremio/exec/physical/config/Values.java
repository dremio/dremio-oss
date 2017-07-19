/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.JSONOptions;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.Leaf;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.beans.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class Values extends AbstractBase implements Leaf, SubScan {

  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Values.class);

  private final JSONOptions content;
  private final BatchSchema schema;

  @JsonCreator
  public Values(
      @JsonProperty("content") JSONOptions content,
      @JsonProperty("schema") BatchSchema schema){
    this.content = content;
    this.schema = Preconditions.checkNotNull(schema);
  }

  public JSONOptions getContent(){
    return content;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitValues(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children.isEmpty();
    return new Values(content, schema);
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
  public List<String> getTableSchemaPath() {
    return Collections.singletonList("values");
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    return schema;
  }

  public BatchSchema getSchema(){
    return schema;
  }

  @JsonIgnore
  @Override
  public List<SchemaPath> getColumns() {
    return GroupScan.ALL_COLUMNS;
  }
}
