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
package com.dremio.sabot.op.scan;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;

import com.dremio.exec.exception.SchemaChangeException;

/**
 * Interface that allows a record reader to modify the current schema.
 *
 * The output mutator interface abstracts ValueVector creation and maintenance away from any particular RecordReader.
 * This means, among other things, that a new RecordReader that shares the same column definitions in a different order
 * does not generate a Schema change event for downstream consumers.
 */
public interface OutputMutator {

  /**
   * Add a ValueVector for new (or existing) field.
   *
   * @param field
   *          The specification of the newly desired vector.
   * @param clazz
   *          The expected ValueVector class. Also allows strongly typed use of this interface.
   *
   * @return The existing or new ValueVector associated with the provided field.
   *
   * @throws SchemaChangeException
   *           If the addition of this field is incompatible with this OutputMutator's capabilities.
   */
  public <T extends ValueVector> T addField(Field field, Class<T> clazz) throws SchemaChangeException;

  default void removeField(Field field) {
    throw new UnsupportedOperationException();
  }

  public ValueVector getVector(String name);

  public Iterable<ValueVector> getVectors();

  public void allocate(int recordCount);

  /**
   * Allows a scanner to request a set of managed block of memory.
   * @return A ArrowBuf that will be released at the end of the current query (and can be resized as desired during use).
   */
  public ArrowBuf getManagedBuffer();

  /**
   *
   * @return the CallBack object for this mutator
   */
  public CallBack getCallBack();

  /**
   * @return true if the schema of the underlying batch changed since the initial schema was built
   * Method also resets the schema change flag.
   */
  boolean getAndResetSchemaChanged();

  /**
   *
   * @return true if the schema of the underlying batch changed since the initial schema was built
   */
  boolean getSchemaChanged();
}
