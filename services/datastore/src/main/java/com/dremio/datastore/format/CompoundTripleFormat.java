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
package com.dremio.datastore.format;

import com.dremio.datastore.DatastoreFatalException;
import com.dremio.datastore.FormatVisitor;
import com.dremio.datastore.format.compound.KeyTriple;

/**
 * Format representing a collection of other formats.
 *
 * @param <K1> - The type of the first key.
 * @param <K2> - The type of the second key.
 * @param <K3> - The type of the third key.
 */
public final class CompoundTripleFormat<K1, K2, K3> implements Format<KeyTriple<K1, K2, K3>> {

  private final String key1Name;
  private final Format<K1> key1Format;
  private final String key2Name;
  private final Format<K2> key2Format;
  private final String key3Name;
  private final Format<K3> key3Format;

  CompoundTripleFormat(String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format, String key3Name, Format<K3> key3Format) {
    this.key1Name = key1Name;
    this.key1Format = key1Format;
    this.key2Name = key2Name;
    this.key2Format = key2Format;
    this.key3Name = key3Name;
    this.key3Format = key3Format;
  }

  @Override
  public Class<KeyTriple<K1, K2, K3>> getRepresentedClass() {
    return (Class) KeyTriple.class;
  }

  @Override
  public <RET> RET apply(FormatVisitor<RET> visitor) throws DatastoreFatalException {
    return visitor.visitCompoundTripleFormat(key1Name, key1Format, key2Name, key2Format, key3Name, key3Format);
  }
}
