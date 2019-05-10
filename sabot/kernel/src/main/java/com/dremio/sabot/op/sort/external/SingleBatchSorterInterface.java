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
package com.dremio.sabot.op.sort.external;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.FunctionContext;

public interface SingleBatchSorterInterface {
  public static TemplateClassDefinition<SingleBatchSorterInterface> TEMPLATE_DEFINITION =
    new TemplateClassDefinition<SingleBatchSorterInterface>(SingleBatchSorterInterface.class, SingleBatchSorterTemplate.class);

  public void setup(FunctionContext context, SelectionVector2 vector2, VectorAccessible incoming) throws SchemaChangeException;
  public void sort(SelectionVector2 vector2);
}
