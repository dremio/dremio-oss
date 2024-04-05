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

import com.dremio.datastore.generator.factory.StringSupplierFactory;
import com.dremio.datastore.generator.supplier.UniqueSupplier;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;

/** Used to generate data for a store with string keys and values */
public class StringStoreGenerator implements DataGenerator<String, String> {
  private final UniqueSupplier<String> nextKey;
  private final UniqueSupplier<String> nextValue;

  public StringStoreGenerator(UniqueSupplierOptions supplierOption) {
    final StringSupplierFactory factory = new StringSupplierFactory(supplierOption);
    nextKey = factory.createSupplier("Str1ng-@key#!=V");
    nextValue = factory.createSupplier("Str1ng-@value#!=V");
  }

  @Override
  public void reset() {
    nextKey.reset();
    nextValue.reset();
  }

  @Override
  public String newKey() {
    return nextKey.get();
  }

  @Override
  public String newVal() {
    return nextValue.get();
  }

  @Override
  public String newValWithNullFields() {
    throw new UnsupportedOperationException("Null fields are not supported in String.");
  }
}
