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
package com.dremio.exec.physical.base;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.graph.GraphVisitor;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSubScan.class);

  private final BatchSchema schema;
  private final List<String> tablePath;

  public AbstractSubScan(String userName, BatchSchema schema, List<String> tablePath) {
    super(userName);
    this.schema = schema;
    this.tablePath = tablePath;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return this;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
    visitor.enter(this);
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext context) {
    return schema;
  }

  @Override
  public List<String> getTableSchemaPath() {
    return tablePath;
  }

  @JsonProperty("schema")
  public BatchSchema getSchema() {
    return schema;
  }
}
