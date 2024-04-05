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
package com.dremio.sabot.op.copier;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

public interface Copier extends AutoCloseable {
  public static TemplateClassDefinition<Copier> TEMPLATE_DEFINITION2 =
      new TemplateClassDefinition<Copier>(Copier.class, CopierTemplate2.class);
  public static TemplateClassDefinition<Copier> TEMPLATE_DEFINITION4 =
      new TemplateClassDefinition<Copier>(Copier.class, CopierTemplate4.class);

  public void setupRemover(
      FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing)
      throws SchemaChangeException;

  public abstract int copyRecords(int index, int recordCount);

  default long getOOMCountDuringAllocation() {
    return 0;
  }

  default void setAllocationDensity(double density) {}

  default long getOOMCountDuringCopy() {
    return 0;
  }
}
