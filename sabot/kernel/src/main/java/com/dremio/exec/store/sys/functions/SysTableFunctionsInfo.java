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

import java.util.List;
import java.util.Objects;

/**
 * Schema for system table entry for sys.functions.
 */
public class SysTableFunctionsInfo {

  public final String name;
  public final String description;
  public final String return_type;
  public final String parameters;

  public SysTableFunctionsInfo(String name, String description, String return_type, String parameters) {
    this.name = name;
    this.description = description;
    this.return_type = return_type;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public String getReturn_type() {
    return return_type;
  }

  public String getDescription() {
    return description;
  }

  public String getParameters() {
    return parameters;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.return_type, this.description, this.parameters);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SysTableFunctionsInfo other = (SysTableFunctionsInfo) o;
    return Objects.equals(this.name, other.getName()) &&
      Objects.equals(this.return_type, other.getReturn_type()) &&
      Objects.equals(this.description, other.getDescription()) &&
      Objects.equals(this.parameters, other.getParameters());
  }

  @Override
  public String toString() {
    return "SysTableFunctionsInfo{" +
      "name='" + name + '\'' +
      ", return_type='" + return_type + '\'' +
      ", description='" + return_type + '\'' +
      ", parameters=" + parameters +
      '}';
  }
}
