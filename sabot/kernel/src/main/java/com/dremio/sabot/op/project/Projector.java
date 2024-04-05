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
package com.dremio.sabot.op.project;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import java.util.List;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.util.TransferPair;

public interface Projector {

  void setup(
      FunctionContext context,
      VectorAccessible incoming,
      VectorAccessible outgoing,
      List<TransferPair> transfers,
      ComplexWriterCreator writerCreator)
      throws SchemaChangeException;

  void projectRecords(final int recordCount);

  TemplateClassDefinition<Projector> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<Projector>(Projector.class, ProjectorTemplate.class);

  public interface ComplexWriterCreator {
    public ComplexWriter addComplexWriter(String name);
  }
}
