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
package com.dremio.datastore.indexed.doughnut;

import com.dremio.datastore.generator.DataGenerator;
import com.dremio.datastore.generator.factory.StringSupplierFactory;
import com.dremio.datastore.generator.supplier.UniqueSupplier;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;

/**
 * Used to generate data for a doughnut store
 */
public class DoughnutStoreGenerator implements DataGenerator<String, Doughnut> {
  private final UniqueSupplier<String> nextKey;
  private final UniqueSupplier<String> nextValue;

  public DoughnutStoreGenerator(UniqueSupplierOptions supplierOption) {
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
  public Doughnut newVal() {
    return new Doughnut(nextValue.get(), "some flavour", 1.0);
  }

  @Override
  public Doughnut newValWithNullFields() {
    return new Doughnut(nextValue.get(), null, 1.0);
  }
}
