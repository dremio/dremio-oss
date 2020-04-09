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

import java.nio.charset.StandardCharsets;

/**
 * Supplier that generates variable-length byte[].
 */
public class VarLengthRawBytesSupplier extends VarLengthSupplier<byte[]> {
  public VarLengthRawBytesSupplier(String prefix) {
    super(prefix);
  }

  @Override
  byte[] convertToTarget(String generated) {
    return generated.getBytes(StandardCharsets.UTF_8);
  }
}
