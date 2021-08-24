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
package com.dremio.exec.store.sys.functions;

import java.util.Objects;

/**
 * Struct for functions parameters.
 */
public class FunctionParameterInfo {
  public final String name;
  public final String data_type;
  public final Boolean is_optional;

  public FunctionParameterInfo(String name, String data_type, Boolean is_optional) {
    this.name = name;
    this.data_type = data_type;
    this.is_optional = is_optional;
  }

  public String getName() {
    return name;
  }

  public String getData_type() {
    return data_type;
  }

  public Boolean getIs_optional() {
    return is_optional;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.data_type, this.is_optional);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FunctionParameterInfo other = (FunctionParameterInfo) o;
    return Objects.equals(this.name, other.getName()) &&
      Objects.equals(this.data_type, other.getData_type()) &&
      Objects.equals(this.is_optional, other.getIs_optional());
  }

  @Override
  public String toString() {
    return new StringBuilder()
      .append("{")
      .append("\"name\":\"" + name + "\",")
      .append("\"data_type\":\"" + data_type + "\",")
      .append("\"is_optional\":\"" + is_optional + "\",")
      .append("}")
      .toString();
  }
}
