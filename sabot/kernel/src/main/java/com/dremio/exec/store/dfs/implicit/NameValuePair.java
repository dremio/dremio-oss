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
package com.dremio.exec.store.dfs.implicit;

import com.dremio.exec.store.dfs.implicit.AdditionalColumnsRecordReader.Populator;

public abstract class NameValuePair<V> implements AutoCloseable {
  final String name;
  final V value;

  public NameValuePair(String name, V value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public V getValue() {
    return value;
  }

  public abstract Populator createPopulator();

  // Return Integer.MAX_VALUE for variable-sized types
  public abstract int getValueTypeSize();

  public abstract byte[] getValueBytes();

  @Override
  public void close() throws Exception {}
}
