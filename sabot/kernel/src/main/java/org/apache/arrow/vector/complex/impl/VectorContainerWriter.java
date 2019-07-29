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
package org.apache.arrow.vector.complex.impl;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class VectorContainerWriter extends AbstractFieldWriter implements ComplexWriter {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VectorContainerWriter.class);

  private final SingleStructWriter structRoot;
  private final SpecialStructVector structVector;
  private final OutputMutator mutator;
  private BufferAllocator allocator;

  public VectorContainerWriter(OutputMutator mutator) {
    this.mutator = mutator;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    structVector = new SpecialStructVector(allocator, mutator.getCallBack());
    structRoot = new SingleStructWriter(structVector);
  }

  @Override
  public Field getField() {
    return structVector.getField();
  }

  @Override
  public int getValueCapacity() {
    return structRoot.getValueCapacity();
  }

  public void setInitialCapacity(int initialCapacity) {
    structRoot.setInitialCapacity(initialCapacity);
  }

  public NonNullableStructVector getStructVector() {
    return structVector;
  }

  @Override
  public void reset() {
    setPosition(0);
  }

  @Override
  public void close() throws Exception {
    clear();
    structRoot.close();
    structVector.close();
  }

  @Override
  public void clear() {
    structRoot.clear();
  }

  public SingleStructWriter getWriter() {
    return structRoot;
  }

  @Override
  public void setValueCount(int count) {
    structRoot.setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    structRoot.setPosition(index);
  }

  @Override
  public void allocate() {
    structRoot.allocate();
  }

  private class SpecialStructVector extends NonNullableStructVector {

    public SpecialStructVector(BufferAllocator allocator, CallBack callback) {
      super("", allocator, callback);
    }

    @Override
    public List<FieldVector> getChildren() {
      return Lists.newArrayList(Iterables.transform(mutator.getVectors(), new Function<ValueVector, FieldVector>() {
        @Nullable
        @Override
        public FieldVector apply(@Nullable ValueVector input) {
          return (FieldVector) input;
        }
      }));
    }

    @Override
    public <T extends FieldVector> T addOrGet(String childName, FieldType fieldType, Class<T> clazz) {
      try {
        Field field = new Field(childName, fieldType, null);
        final FieldVector v = mutator.addField(field, clazz);
        putChild(childName, v);
        return this.typeify(v, clazz);
      } catch (SchemaChangeException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public <T extends FieldVector> T getChild(String name, Class<T> clazz) {
      final ValueVector v = mutator.getVector(name.toLowerCase());
      if (v == null) {
        return null;
      }
      return typeify(v, clazz);
    }
  }

  @Override
  public StructWriter rootAsStruct() {
    return structRoot;
  }

  @Override
  public ListWriter rootAsList() {
    throw new UnsupportedOperationException(
        "Dremio doesn't support objects whose first level is a scalar or array.  Objects must start as maps.");
  }
}
