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
package com.dremio.dac.model.common;

import com.dremio.dac.proto.model.dataset.DataType;

/**
 * Class representing a dataset field
 */
public class Field {
  private String name;
  private DataType type;

  public Field() {
  }

  public Field(String name, DataType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "Field{" +
      "name='" + name + '\'' +
      ", type=" + type +
      '}';
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(DataType type) {
    this.type = type;
  }
}
