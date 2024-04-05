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
package com.dremio.sabot.op.receiver.merging;

import static com.dremio.exec.compile.sig.GeneratorMapping.GM;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.compile.sig.MappingSet;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;

public interface Merger {

  public abstract void doSetup(
      FunctionContext context, VectorAccessible incoming, VectorAccessible outgoing)
      throws SchemaChangeException;

  public abstract int doEval(int leftIndex, int rightIndex);

  public abstract void doCopy(int inIndex, int outIndex);

  public static TemplateClassDefinition<Merger> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(Merger.class, MergerTemplate.class);

  public final MappingSet compareMapping =
      new MappingSet(
          "leftIndex",
          "rightIndex",
          GM("doSetup", "doCompare", null, null),
          GM("doSetup", "doCompare", null, null));

  public final MappingSet copyMapping =
      new MappingSet(
          "inIndex",
          "outIndex",
          GM("doSetup", "doCopy", null, null),
          GM("doSetup", "doCopy", null, null));
}
