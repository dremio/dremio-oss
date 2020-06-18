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

package com.dremio.exec.store.ischema.writers;

import java.util.function.Function;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Charsets;

/**
 * Writes "string" field types.
 *
 * @param <V> type to extract string from
 */
final class StringFieldWriter<V> implements FieldWriter<V> {

  private final NullableVarCharHolder holder = new NullableVarCharHolder();

  private final Function<V, String> stringExtractor;
  private final VarCharVector varCharVector;

  private ArrowBuf managedBuffer;

  StringFieldWriter(
    Field field,
    OutputMutator outputMutator,
    Function<V, String> stringExtractor
  ) {
    this.managedBuffer = outputMutator.getManagedBuffer();
    this.stringExtractor = stringExtractor;

    varCharVector = outputMutator.addField(field, VarCharVector.class);
  }

  @Override
  public void allocate() {
    varCharVector.allocateNew();
  }

  @Override
  public void setValueCount(int i) {
    varCharVector.setValueCount(i);
  }

  @Override
  public void writeField(V value, int recordIndex) {
    if (value == null) {
      return;
    }

    final String string = stringExtractor.apply(value);
    if (string == null) {
      return;
    }

    final byte[] bytes = string.getBytes(Charsets.UTF_8);
    managedBuffer = managedBuffer.reallocIfNeeded(bytes.length);
    managedBuffer.clear();
    managedBuffer.writeBytes(bytes);

    holder.isSet = 1;
    holder.buffer = managedBuffer;
    holder.start = 0;
    holder.end = bytes.length;

    varCharVector.setSafe(recordIndex, holder);
  }

}
