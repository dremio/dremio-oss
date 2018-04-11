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
package com.dremio.exec.physical.base;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordWriter;

public abstract class AbstractWriter extends AbstractSingle implements Writer{

  private final WriterOptions options;

  /**
   * @param child Child POP.
   * @param userName Name of the user whom to impersonate when writing the table to storage plugin.
   */
  public AbstractWriter(PhysicalOperator child, String userName, WriterOptions options) {
    super(child, userName);
    this.options = options;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitWriter(this, value);
  }

  @Override
  protected BatchSchema constructSchema(FunctionLookupContext functionLookupContext) {
    return RecordWriter.SCHEMA;
  }

  public WriterOptions getOptions() {
    return options;
  }
}
