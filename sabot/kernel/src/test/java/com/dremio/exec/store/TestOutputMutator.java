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
package com.dremio.exec.store;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestOutputMutator implements OutputMutator, Iterable<VectorWrapper<?>>, AutoCloseable {

  private final VectorContainer container = new VectorContainer();
  private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  private final BufferAllocator allocator;
  private final BufferManager bufferManager;

  public TestOutputMutator(BufferAllocator allocator) {
    this.allocator = allocator;
    this.bufferManager = new BufferManagerImpl(allocator);
  }

  public void removeField(Field field) throws SchemaChangeException {
    ValueVector vector = fieldVectorMap.remove(field.getName().toLowerCase());
    if (vector == null) {
      throw new SchemaChangeException("Failure attempting to remove an unknown field.");
    }
    container.remove(vector);
    vector.close();
  }

  public void addField(ValueVector vector) {
    container.add(vector);
    fieldVectorMap.put(vector.getField().getName().toLowerCase(), vector);
  }

  public void finalizeContainer(int recordCount){
    container.buildSchema();
    container.setRecordCount(recordCount);
  }

  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  public void clear() {

  }

  @Override
  public void allocate(int recordCount) {
    return;
  }

  @Override
  public <T extends ValueVector> T addField(Field field, Class<T> clazz) throws SchemaChangeException {
    ValueVector v = fieldVectorMap.get(field.getName().toLowerCase());
    if (v == null || v.getClass() != clazz) {
      v = TypeHelper.getNewVector(field, allocator);
      if (!clazz.isAssignableFrom(v.getClass())) {
        throw new SchemaChangeException(String.format("The class that was provided %s does not correspond to the expected vector type of %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
      }
      addField(v);
    }
    return (T) v;
  }

  @Override
  public ValueVector getVector(String name) {
    return fieldVectorMap.get(name.toLowerCase());
  }

  @Override
  public Collection<ValueVector> getVectors() {
    return Lists.newArrayList(Iterables.transform(container, new Function<VectorWrapper<?>, ValueVector>() {
      @Nullable
      @Override
      public ValueVector apply(@Nullable VectorWrapper<?> input) {
        return input.getValueVector();
      }
    }));
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public CallBack getCallBack() {
    return null;
  }

  public VectorContainer getContainer() {
    return container;
  }

  public boolean getAndResetSchemaChanged() {
    return false;
  }

  public boolean getSchemaChanged() {
    return false;
  }

  public Map<String,ValueVector> getFieldVectorMap() {
    return fieldVectorMap;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(container, bufferManager);
  }
}
