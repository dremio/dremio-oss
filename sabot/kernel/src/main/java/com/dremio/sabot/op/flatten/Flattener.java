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
package com.dremio.sabot.op.flatten;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.RepeatedValueVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.project.Projector.ComplexWriterCreator;

public interface Flattener {
  public void setup(
      BufferAllocator allocator,
      FunctionContext context,
      VectorAccessible incoming,
      VectorAccessible outgoing,
      List<TransferPair> transfers,
      ComplexWriterCreator writerCreator,
      long outputMemoryLimit,
      long outputBatchSize
      );

  public interface Monitor {
    /**
     * Get the required buffer size for the specified number of records.
     * {@see ValueVector#getBufferSizeFor(int)} for the meaning of this.
     *
     * @param recordCount the number of records processed so far
     * @return the buffer size the vectors report as being in use
     */
    public int getBufferSizeFor(int recordCount);
  };

  public int flattenRecords(int recordCount, int firstOutputIndex, Monitor monitor);

  public void setFlattenField(RepeatedValueVector repeatedColumn);
  public RepeatedValueVector getFlattenField();
  public void resetGroupIndex();

  public static final TemplateClassDefinition<Flattener> TEMPLATE_DEFINITION = new TemplateClassDefinition<Flattener>(Flattener.class, FlattenTemplate.class);
}
