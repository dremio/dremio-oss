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
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.scan.MutatorSchemaChangeCallBack;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;


/**
 * Used for sampling, etc
 * TODO rename this class, since it used for more than sampling now
 */
public class SampleMutator implements OutputMutator, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SampleMutator.class);

  private final Map<String, ValueVector> fieldVectorMap = Maps.newHashMap();
  private final MutatorSchemaChangeCallBack callBack = new MutatorSchemaChangeCallBack();
  private final VectorContainer container = new VectorContainer();
  private final BufferAllocator allocator;
  private final BufferManager bufferManager;

  public SampleMutator(BufferAllocator allocator) {
    this.allocator = allocator;
    this.bufferManager = new BufferManagerImpl(allocator);
  }

  public void removeField(Field field) throws SchemaChangeException {
    ValueVector vector = fieldVectorMap.remove(field.getName().toLowerCase());
    if (vector == null) {
      throw new SchemaChangeException("Failure attempting to remove an unknown field.");
    }
    try(ValueVector v = vector) {
      container.remove(vector);
    }
  }

  public <T extends ValueVector> T addField(Field field, Class<T> clazz) throws SchemaChangeException {
    ValueVector v = fieldVectorMap.get(field.getName().toLowerCase());
    if (v == null || v.getClass() != clazz) {
      // Field does not exist--add it to the map and the output container.
      v = TypeHelper.getNewVector(field, allocator, callBack);
      if (!clazz.isAssignableFrom(v.getClass())) {
        throw new SchemaChangeException(
                String.format(
                        "The class that was provided, %s, does not correspond to the "
                                + "expected vector type of %s.",
                        clazz.getSimpleName(), v.getClass().getSimpleName()));
      }

      String fieldName = field.getName();
      addVectorForFieldName(v, fieldName);
      // Added new vectors to the container--mark that the schema has changed.
    }

    return clazz.cast(v);
  }

  private void addVectorForFieldName(ValueVector v, String fieldName) {
    final ValueVector old = fieldVectorMap.put(fieldName.toLowerCase(), v);
    if (old != null) {
      container.replace(old, v);
    } else {
      container.add(v);
    }
  }

  public void addVector(ValueVector vector) {
    Preconditions.checkArgument(vector != null, "Invalid vector");
    Preconditions.checkArgument(vector.getField() != null, "Invalid field");
    addVectorForFieldName(vector, vector.getField().getName());
  }

  @Override
  public ValueVector getVector(String name) {
    return fieldVectorMap.get(name.toLowerCase());
  }

  @Override
  public Collection<ValueVector> getVectors() {
    return fieldVectorMap.values();
  }

  @Override
  public void allocate(int recordCount) {
    container.allocateNew();
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public CallBack getCallBack() {
    return callBack;
  }

  public Map<String,ValueVector> getFieldVectorMap() {
    return fieldVectorMap;
  }

  public VectorContainer getContainer() {
    return container;
  }

  @Override
  public boolean getAndResetSchemaChanged() {
    return callBack.getSchemaChangedAndReset() || container.isNewSchema();
  }

  @Override
  public boolean getSchemaChanged() {
    return container.isNewSchema() || callBack.getSchemaChanged();
  }

  /**
   * Since this OutputMutator is passed by TextRecordReader to get the header out
   * the mutator might not get cleaned up elsewhere. TextRecordReader will call
   * this method to clear any allocations
   */
  public void close() {
    logger.debug("closing mutator");
    for (final ValueVector v : fieldVectorMap.values()) {
      v.clear();
    }
    fieldVectorMap.clear();
    container.clear();
    bufferManager.close();
  }
}
