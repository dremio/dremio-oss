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
package org.apache.arrow.vector.complex.impl;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.MapVector;
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

  private final SingleMapWriter mapRoot;
  private final SpecialMapVector mapVector;
  private final OutputMutator mutator;

  public VectorContainerWriter(OutputMutator mutator) {
    this.mutator = mutator;
    mapVector = new SpecialMapVector(mutator.getCallBack());
    mapRoot = new SingleMapWriter(mapVector);
  }

  @Override
  public Field getField() {
    return mapVector.getField();
  }

  @Override
  public int getValueCapacity() {
    return mapRoot.getValueCapacity();
  }

  public void setInitialCapacity(int initialCapacity) {
    mapRoot.setInitialCapacity(initialCapacity);
  }

  public MapVector getMapVector() {
    return mapVector;
  }

  @Override
  public void reset() {
    setPosition(0);
  }

  @Override
  public void close() throws Exception {
    clear();
    mapRoot.close();
    mapVector.close();
  }

  @Override
  public void clear() {
    mapRoot.clear();
  }

  public SingleMapWriter getWriter() {
    return mapRoot;
  }

  @Override
  public void setValueCount(int count) {
    mapRoot.setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    mapRoot.setPosition(index);
  }

  @Override
  public void allocate() {
    mapRoot.allocate();
  }

  private class SpecialMapVector extends MapVector {

    public SpecialMapVector(CallBack callback) {
      super("", null, callback);
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
  public MapWriter rootAsMap() {
    return mapRoot;
  }

  @Override
  public ListWriter rootAsList() {
    throw new UnsupportedOperationException(
        "Dremio doesn't support objects whose first level is a scalar or array.  Objects must start as maps.");
  }
}
