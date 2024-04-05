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
package com.dremio.exec.record;

import com.dremio.common.expression.BasePath;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;

public interface VectorWrapper<T extends ValueVector> extends AutoCloseable {

  public Class<T> getVectorClass();

  public Field getField();

  public T getValueVector();

  public T[] getValueVectors();

  public boolean isHyper();

  public void clear();

  @Override
  public void close();

  public VectorWrapper<T> cloneAndTransfer(BufferAllocator allocator, CallBack callback);

  public VectorWrapper<?> getChildWrapper(int[] ids);

  public void transfer(VectorWrapper<?> destination);

  /**
   * Traverse the object graph and determine whether the provided BasePath matches data within the
   * Wrapper. If so, return a TypedFieldId associated with this path.
   *
   * @return TypedFieldId
   */
  public TypedFieldId getFieldIdIfMatches(int id, BasePath expectedPath);
}
