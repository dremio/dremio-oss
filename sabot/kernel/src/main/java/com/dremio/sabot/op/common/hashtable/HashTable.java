/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.common.hashtable;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.FunctionContext;

public interface HashTable extends AutoCloseable {

  public static TemplateClassDefinition<HashTable> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<HashTable>(HashTable.class, HashTableTemplate.class);

  /**
   * The initial default capacity of the hash table (in terms of number of buckets).
   */
  static final public int DEFAULT_INITIAL_CAPACITY = 1 << 16;

  /**
   * The maximum capacity of the hash table (in terms of number of buckets).
   */
  static final public int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The default load factor of a hash table.
   */
  static final public float DEFAULT_LOAD_FACTOR = 0.75f;

  static public enum PutStatus {KEY_PRESENT, KEY_ADDED, PUT_FAILED;}

  /**
   * The batch size used for internal batch holders
   */
  static final public int BATCH_SIZE = Character.MAX_VALUE + 1;
  static final public int BATCH_MASK = 0x0000FFFF;

  /** Variable width vector size in bytes */
  public static final int VARIABLE_WIDTH_VECTOR_SIZE = 50 * BATCH_SIZE;

  void setup(HashTableConfig htConfig, FunctionContext context, BufferAllocator allocator,
      VectorAccessible incomingBuild, VectorAccessible incomingProbe,
      VectorAccessible outgoing, VectorContainer htContainerOrig,
      BatchAddedListener listener);

  int put(int incomingRowIdx);

  int containsKey(int incomingRowIdx, boolean isProbe);

  void getStats(HashTableStats stats);

  int size();

  boolean isEmpty();

  void outputKeys(int batchIdx, VectorContainer outContainer);

  public interface BatchAddedListener {
    public void batchAdded();
  }

  BatchAddedListener NOOP_LISTENER = new BatchAddedListener(){
    @Override
    public void batchAdded() {
    }};
}


