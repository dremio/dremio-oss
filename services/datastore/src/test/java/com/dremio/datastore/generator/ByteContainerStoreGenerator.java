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

import com.dremio.datastore.generator.factory.ByteContainerSupplierFactory;
import com.dremio.datastore.generator.factory.StringSupplierFactory;
import com.dremio.datastore.generator.supplier.UniqueSupplier;
import com.dremio.datastore.generator.supplier.UniqueSupplierOptions;
import java.util.Arrays;

/** Generates data for a byte store. */
public class ByteContainerStoreGenerator
    implements DataGenerator<String, ByteContainerStoreGenerator.ByteContainer> {
  private final UniqueSupplier<String> nextKey;
  private final UniqueSupplier<ByteContainer> nextValue;

  public ByteContainerStoreGenerator(UniqueSupplierOptions supplierOption) {
    final StringSupplierFactory stringFactory = new StringSupplierFactory(supplierOption);
    final ByteContainerSupplierFactory byteFactory =
        new ByteContainerSupplierFactory(supplierOption);
    nextKey = stringFactory.createSupplier("String@key-1ab3$5P_");
    nextValue = byteFactory.createSupplier("Bytes@value-1ab3$5P_");
  }

  @Override
  public String newKey() {
    return nextKey.get();
  }

  @Override
  public ByteContainer newVal() {
    return nextValue.get();
  }

  @Override
  public ByteContainer newValWithNullFields() {
    throw new UnsupportedOperationException("Null fields are not supported in ByteContainer.");
  }

  @Override
  public void reset() {
    nextKey.reset();
    nextValue.reset();
  }

  /**
   * Wraps the byte[] so equality can be checked element-wise.
   *
   * <p>Also, tests wrapped formats.
   */
  public static class ByteContainer {
    private final byte[] bytes;

    public ByteContainer(byte[] bytes) {
      this.bytes = bytes.clone();
    }

    public byte[] getBytes() {
      return bytes.clone();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ByteContainer)) {
        return false;
      }

      final ByteContainer other = (ByteContainer) obj;
      return Arrays.equals(other.bytes, this.bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(this.bytes);
    }
  }
}
