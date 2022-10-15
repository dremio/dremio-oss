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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import com.dremio.common.expression.Describer;

public class Reallocators {

  private Reallocators(){}

  public static interface Reallocator {
    long addr();
    long ensure(int size);
    long max();
    void setCount(int count);
    void ensureValidityAndOffsets(int count);
  }

  private static class VarBinaryReallocator implements Reallocator {
    private final VarBinaryVector varbinary;

    public VarBinaryReallocator(VarBinaryVector vector) {
      this.varbinary = vector;
    }

    @Override
    public long ensure(int size) {
      while(varbinary.getByteCapacity() < size){
        varbinary.reallocDataBuffer();
      }
      return addr();
    }

    @Override
    public long addr() {
      return varbinary.getDataBufferAddress();
    }

    @Override
    public long max() {
      return addr() + varbinary.getByteCapacity();
    }

    @Override
    public void setCount(int count) {
      varbinary.setLastSet(count);
    }

    @Override
    public void ensureValidityAndOffsets(int count) {
      while (varbinary.getValueCapacity() < count) {
        varbinary.reallocValidityAndOffsetBuffers();
      }
    }
  }

  private static class VarCharReallocator implements Reallocator {
    private final VarCharVector varchar;

    public VarCharReallocator(VarCharVector vector) {
      this.varchar = vector;
    }

    @Override
    public long ensure(int size) {
      while(varchar.getByteCapacity() < size){
        varchar.reallocDataBuffer();
      }
      return addr();
    }

    @Override
    public long addr() {
      return varchar.getDataBufferAddress();
    }

    @Override
    public long max() {
      return addr() + varchar.getByteCapacity();
    }

    @Override
    public void setCount(int count) {
      varchar.setLastSet(count);
    }

    @Override
    public void ensureValidityAndOffsets(int count) {
      while (varchar.getValueCapacity() < count) {
        varchar.reallocValidityAndOffsetBuffers();
      }
    }
  }

  public static Reallocator getReallocator(FieldVector vect){
    if(vect instanceof VarCharVector){
      return new VarCharReallocator(((VarCharVector) vect));
    }else if(vect instanceof VarBinaryVector){
      return new VarBinaryReallocator(((VarBinaryVector) vect));
    }else{
      throw new IllegalStateException("Invalid vector: " + Describer.describe(vect.getField()));
    }
  }
}
