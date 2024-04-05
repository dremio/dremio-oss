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

import com.dremio.datastore.generator.factory.RawBytesSupplierFactory;
import com.dremio.datastore.generator.factory.StringSupplierFactory;
import com.dremio.datastore.generator.supplier.UniqueSupplier;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;

/** Generates data for a raw bytes store. */
public class RawByteStoreGenerator implements DataGenerator<String, byte[]> {
  private final UniqueSupplier<String> nextKey;
  private final UniqueSupplier<byte[]> nextValue;

  public RawByteStoreGenerator(UniqueSupplierOptions supplierOption) {
    final StringSupplierFactory stringFactory = new StringSupplierFactory(supplierOption);
    final RawBytesSupplierFactory byteFactory = new RawBytesSupplierFactory(supplierOption);
    this.nextKey = stringFactory.createSupplier("String@key-2cd4$6P_");
    this.nextValue = byteFactory.createSupplier("RawBytes@value-2cd4$6P_");
  }

  @Override
  public String newKey() {
    return nextKey.get();
  }

  @Override
  public byte[] newVal() {
    return nextValue.get();
  }

  @Override
  public byte[] newValWithNullFields() {
    throw new UnsupportedOperationException("Null fields are not supported with byte[].");
  }

  @Override
  public void reset() {
    nextKey.reset();
    nextValue.reset();
  }
}
