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
package com.dremio.sabot.join.hash;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableBigIntVector;

import com.dremio.common.types.Types;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.sabot.Generator;

public class EmptyGenerator implements Generator {

  private final VectorContainer c;

  public EmptyGenerator(BufferAllocator allocator){
    c = new VectorContainer(allocator);
    c.addOrGet("key", Types.optional(MinorType.BIGINT), NullableBigIntVector.class);
    c.addOrGet("value", Types.optional(MinorType.BIGINT), NullableBigIntVector.class);
    c.buildSchema(SelectionVectorMode.NONE);
  }

  @Override
  public void close() throws Exception {
    c.close();
  }

  @Override
  public VectorAccessible getOutput() {
    return c;
  }

  @Override
  public int next(int records) {
    c.allocateNew();
    return c.setAllCount(0);
  }

}
