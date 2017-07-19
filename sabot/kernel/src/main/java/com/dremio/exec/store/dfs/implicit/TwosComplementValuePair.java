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
package com.dremio.exec.store.dfs.implicit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableDecimalVector;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader.Populator;
import com.dremio.sabot.op.scan.OutputMutator;

import io.netty.buffer.ArrowBuf;

public class TwosComplementValuePair extends NameValuePair<byte[]>{

  private final int scale;
  private final int precision;
  private ArrowBuf buf;

  public TwosComplementValuePair(BufferAllocator allocator, Field field, byte[] value) {
    super(field.getName(), DecimalTools.signExtend16(value));
    CompleteType type = CompleteType.fromField(field);
    scale = type.getScale();
    precision = type.getPrecision();
    buf = allocator.buffer(16);
    buf.setBytes(0, value);
  }

  @Override
  public Populator createPopulator() {
    return new BigDecimalPopulator();
  }

  private final class BigDecimalPopulator implements Populator, AutoCloseable {
    private NullableDecimalVector.Mutator mutator;
    private NullableDecimalVector vector;

    public void setup(OutputMutator output){
      vector = (NullableDecimalVector)output.getVector(name);
      if (vector == null) {
        vector = output.addField(new Field(name, true, new Decimal(precision, scale), null), NullableDecimalVector.class);
      }
      mutator = vector.getMutator();
    }

    public void populate(final int count){
      final byte[] value = TwosComplementValuePair.this.value;

      if(value != null) {
        for (int i = 0; i < count; i++) {
          mutator.setSafe(i, buf);
        }
      }
      mutator.setValueCount(count);
    }

    public void allocate(){
      vector.allocateNew();
    }

    @Override
    public void close() throws Exception {
      try{
        AutoCloseables.close(vector, buf);
      }finally{
        buf = null;
      }
    }
  }

}
