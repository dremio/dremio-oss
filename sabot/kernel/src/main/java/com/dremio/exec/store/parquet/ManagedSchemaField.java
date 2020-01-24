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

package com.dremio.exec.store.parquet;

/**
 * Field definition of the managed schema
 */
public class ManagedSchemaField {
  private final String name;
  private final String type;
  private final int length;
  private final int scale;

  public ManagedSchemaField(final String name, final String type, final int length, final int scale) {
    this.name = name;
    this.type = type;
    this.length = length;
    this.scale = scale;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public int getLength() {
    return length;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public String toString() {
    return "ManagedSchemaField{" +
      "name='" + name + '\'' +
      ", type='" + type + '\'' +
      ", length=" + length +
      ", scale=" + scale +
      '}';
  }
}
