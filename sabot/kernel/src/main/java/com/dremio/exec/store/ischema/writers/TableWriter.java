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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Writes a table.
 *
 * @param <V> value type
 */
public abstract class TableWriter<V> {

  private final Iterator<V> messageIterator;
  private final Set<String> selectedFields;
  private final List<FieldWriter<V>> fieldWriters;

  TableWriter(Iterator<V> messageIterator, Set<String> selectedFields) {
    this.messageIterator = messageIterator;
    this.selectedFields = selectedFields;
    this.fieldWriters = new ArrayList<>();
  }

  /**
   * Initialize the table writer.
   *
   * <p>Add ordered field writers using {@link #addStringWriter} and {@link #addIntWriter}.
   *
   * @param outputMutator output mutator
   */
  public abstract void init(OutputMutator outputMutator);

  protected void addStringWriter(
      Field field, OutputMutator outputMutator, Function<V, String> stringExtractor) {
    if (selectedFields.contains(field.getName())) {
      fieldWriters.add(new StringFieldWriter<>(field, outputMutator, stringExtractor));
    }
  }

  protected void addIntWriter(
      Field field, OutputMutator outputMutator, Function<V, Integer> intExtractor) {
    if (selectedFields.contains(field.getName())) {
      fieldWriters.add(new IntFieldWriter<>(field, outputMutator, intExtractor));
    }
  }

  /**
   * Write records.
   *
   * @param numRecords number of records to write
   * @return number of records written
   */
  public int write(int numRecords) {
    boolean allocated = false;
    int count = 0;

    while (messageIterator.hasNext() && count < numRecords) {
      if (!allocated) {
        fieldWriters.forEach(FieldWriter::allocate);
        allocated = true;
      }

      // Gets next message from catalog service. The underlying stream observer uses automatic flow
      // control. Note that,
      // " ... Netty translates the per-message flow control to byte-based flow control, plus some
      // buffering. So Netty,
      // for instance, won't turn isReady() from true to false unless you end up writing at least
      // 64K worth of
      // messages. ...", so this could degenerate to one #request per call to this #write method for
      // messages > 64 KB.
      // batch_schema in TableSchema message with 800 VARCHAR fields is roughly 64 KB.
      final V value = messageIterator.next();
      final int currentCount = count;
      fieldWriters.forEach(fieldWriter -> fieldWriter.writeField(value, currentCount));
      count++;
    }

    if (count != 0) {
      final int finalCount = count;
      fieldWriters.forEach(fieldWriter -> fieldWriter.setValueCount(finalCount));
    }
    return count;
  }
}
