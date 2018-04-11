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
package com.dremio.sabot.op.scan;

import java.util.Collection;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;

import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorContainer;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

public class VectorContainerMutator implements OutputMutator {

  private static CallBack NOOP = new CallBack(){public void doWork(){}};

  private final VectorContainer container;
  private final BufferManager manager;

  public VectorContainerMutator(VectorContainer container){
    this(container, null);
  }

  public VectorContainerMutator(VectorContainer container, BufferManager manager){
    this.container = container;
    this.manager = manager;
  }

  @Override
  public <T extends ValueVector> T addField(Field field, Class<T> clazz) throws SchemaChangeException {
    return container.addOrGet(field);
  }

  @Override
  public ValueVector getVector(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void allocate(int recordCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getManagedBuffer() {
    Preconditions.checkNotNull(manager, "Must construct a vector container mutator with a buffer manager to use it.");
    return manager.getManagedBuffer();
  }

  @Override
  public Collection<ValueVector> getVectors() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallBack getCallBack() {
    return NOOP;
  }

  @Override
  public boolean isSchemaChanged() {
    return false;
  }
}
