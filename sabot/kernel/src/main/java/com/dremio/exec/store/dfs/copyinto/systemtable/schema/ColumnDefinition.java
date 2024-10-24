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
package com.dremio.exec.store.dfs.copyinto.systemtable.schema;

import org.apache.iceberg.types.Type;

public class ColumnDefinition {
  private final String name;
  private final int id;
  private final Type type;
  private final boolean isOptional;
  private final boolean isHidden;

  public ColumnDefinition(String name, int id, Type type, boolean isOptional, boolean isHidden) {
    this.name = name;
    this.id = id;
    this.type = type;
    this.isOptional = isOptional;
    this.isHidden = isHidden;
  }

  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  public Type getType() {
    return type;
  }

  public boolean isOptional() {
    return isOptional;
  }

  public boolean isHidden() {
    return isHidden;
  }
}
