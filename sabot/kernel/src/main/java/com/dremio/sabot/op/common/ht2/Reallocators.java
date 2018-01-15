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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.NullableVarCharVector;
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
  }

  private static class VarBinaryReallocator implements Reallocator {
    private final NullableVarBinaryVector.Mutator mutator;
    private final VarBinaryVector varbinary;

    public VarBinaryReallocator(NullableVarBinaryVector vector) {
      this.mutator = vector.getMutator();
      this.varbinary = vector.getValuesVector();
    }

    @Override
    public long ensure(int size) {
      while(varbinary.getByteCapacity() < size){
        varbinary.reAlloc();
      }
      return addr();
    }

    @Override
    public long addr() {
      return varbinary.getBuffer().memoryAddress();
    }

    @Override
    public long max() {
      return addr() + varbinary.getByteCapacity();
    }

    @Override
    public void setCount(int count) {
      mutator.setLastSet(count);
    }

  }

  private static class VarCharReallocator implements Reallocator {
    private final NullableVarCharVector.Mutator mutator;
    private final VarCharVector varchar;

    public VarCharReallocator(NullableVarCharVector vector) {
      this.varchar = vector.getValuesVector();
      this.mutator = vector.getMutator();
    }

    @Override
    public long ensure(int size) {
      while(varchar.getByteCapacity() < size){
        varchar.reAlloc();
      }
      return varchar.getBuffer().memoryAddress();
    }

    @Override
    public long addr() {
      return varchar.getBuffer().memoryAddress();
    }

    @Override
    public long max() {
      return addr() + varchar.getByteCapacity();
    }


    @Override
    public void setCount(int count) {
      mutator.setLastSet(count);
    }
  }

  public static Reallocator getReallocator(FieldVector vect){
    if(vect instanceof NullableVarCharVector){
      return new VarCharReallocator(((NullableVarCharVector) vect));
    }else if(vect instanceof NullableVarBinaryVector){
      return new VarBinaryReallocator(((NullableVarBinaryVector) vect));
    }else{
      throw new IllegalStateException("Invalid vector: " + Describer.describe(vect.getField()));
    }
  }
}
