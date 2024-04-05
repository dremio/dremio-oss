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

import com.dremio.sabot.op.scan.OutputMutator;
import java.util.function.Function;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Writes "int" field types.
 *
 * @param <V> type to extract int from
 */
final class IntFieldWriter<V> implements FieldWriter<V> {

  private final Function<V, Integer> intExtractor;
  private final IntVector intVector;

  IntFieldWriter(Field field, OutputMutator outputMutator, Function<V, Integer> intExtractor) {
    this.intExtractor = intExtractor;

    intVector = outputMutator.addField(field, IntVector.class);
  }

  @Override
  public void writeField(V value, int outboundIndex) {
    if (value == null) {
      return;
    }

    final Integer integer = intExtractor.apply(value);
    if (integer == null) {
      return;
    }

    intVector.setSafe(outboundIndex, integer);
  }

  @Override
  public void allocate() {
    intVector.allocateNew();
  }

  @Override
  public void setValueCount(int i) {
    intVector.setValueCount(i);
  }
}
