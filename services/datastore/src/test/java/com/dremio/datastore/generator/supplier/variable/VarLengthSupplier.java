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
package com.dremio.datastore.generator.supplier.variable;

import com.dremio.datastore.generator.supplier.UniqueSupplier;

/**
 * Variable-length values supplier.
 *
 * @param <T> type of variable-length values to generate.
 */
public abstract class VarLengthSupplier<T> implements UniqueSupplier<T> {
  private int count;
  private final String prefix;

  public VarLengthSupplier(String prefix) {
    this.count = 0;
    this.prefix = prefix;
  }

  abstract T convertToTarget(String generated);

  @Override
  public void reset() {
    count = 0;
  }

  @Override
  public T get() {
    count++;

    final StringBuilder builder = new StringBuilder(prefix);

    if ((count % 2) == 0) {
      builder.append(((count % 4) == 0) ? (count * 100) : count * 100000);
    } else {
      builder.append(((count % 3) == 0) ? (count * 1000) : ((count % 5) == 0) ? count * 10 : count);
    }

    return convertToTarget(builder.toString());
  }
}
