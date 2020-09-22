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

/**
 * Field writer.
 *
 * @param <V> value type to extract field from
 */
interface FieldWriter<V> {

  /**
   * Allocate the underlying vector.
   */
  void allocate();

  /**
   * Set the value count of the underlying vector.
   *
   * @param i value count
   */
  void setValueCount(int i);

  /**
   * Write the field from the given value, at the index.
   *
   * @param value value
   * @param recordIndex record index
   */
  void writeField(V value, int recordIndex);
}
