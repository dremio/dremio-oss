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
package com.dremio.datastore.generator;

import java.util.Comparator;
import java.util.UUID;
import java.util.function.Supplier;

import com.dremio.datastore.generator.supplier.fixed.FixedLengthStringSupplier;

/**
 * Generates data for UUIDStores.
 *
 * Does not produce UUIDs in a sorted order.
 */
public class UUIDStoreGenerator implements DataGenerator<UUID, String> {
  private final Supplier<String> nextValue = new FixedLengthStringSupplier("Str1ng-@uuid-value#!=V");

  @Override
  public UUID newKey() {
    return UUID.randomUUID();
  }

  @Override
  public String newVal() {
    return nextValue.get();
  }

  @Override
  public String newValWithNullFields() {
    throw new UnsupportedOperationException("Null fields are not supported in UUID.");
  }

  @Override
  public Comparator<UUID> getComparator() {
    return new UUIDComparator();
  }

  private class UUIDComparator implements Comparator<UUID> {
    @Override
    public int compare(UUID o1, UUID o2) {
      return o1.toString().compareTo(o2.toString());
    }
  }
}
