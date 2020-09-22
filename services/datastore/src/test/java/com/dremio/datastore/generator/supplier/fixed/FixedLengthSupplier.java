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
package com.dremio.datastore.generator.supplier.fixed;

import com.dremio.datastore.generator.supplier.UniqueSupplier;

/**
 * Fixed-length values supplier.
 *
 * @param <T> type of fixed-length values to generate.
 */
public abstract class FixedLengthSupplier<T> implements UniqueSupplier<T> {
  private static final int FIXED_LENGTH = Integer.toString(Integer.MAX_VALUE).length();
  private static final String FIXED_LENGTH_FILLER = "Z";

  private int count;
  private final String prefix;

  public FixedLengthSupplier(String prefix) {
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
    final int delta = FIXED_LENGTH - Integer.toString(count).length();
    final StringBuilder builder = new StringBuilder(prefix + count);

    for (int i = 0; i < delta; i++) {
      builder.append(FIXED_LENGTH_FILLER);
    }
    return convertToTarget(builder.toString());
  }
}
