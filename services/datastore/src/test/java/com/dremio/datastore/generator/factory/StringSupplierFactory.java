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
package com.dremio.datastore.generator.factory;

import com.dremio.datastore.generator.supplier.UniqueSupplier;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import com.dremio.datastore.generator.supplier.fixed.FixedLengthStringSupplier;
import com.dremio.datastore.generator.supplier.variable.VarLengthStringSupplier;

/** Factory to supply the correct unique String supplier. */
public class StringSupplierFactory {
  private final UniqueSupplierOptions supplier;

  public StringSupplierFactory(UniqueSupplierOptions supplier) {
    this.supplier = supplier;
  }

  public UniqueSupplier<String> createSupplier(String prefix) {
    switch (supplier) {
      case VARIABLE_LENGTH:
        return new VarLengthStringSupplier(prefix);
      case FIXED_LENGTH:
        return new FixedLengthStringSupplier(prefix);
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported supplier option %s.", supplier.name()));
    }
  }
}
