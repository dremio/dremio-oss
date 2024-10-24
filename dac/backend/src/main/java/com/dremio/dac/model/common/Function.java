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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** User-defined function */
@JsonIgnoreProperties(
    value = {"links"},
    allowGetters = true)
public class Function {
  // TODO: DX-94503 To show Arctic UDFs in UI, we need to add more properties e.g. function args,
  // body, etc.
  private final String id;
  private final List<String> fullPathList;

  @JsonCreator
  public Function(
      @JsonProperty("id") String id, @JsonProperty("fullPathList") List<String> fullPathList) {
    this.id = id;
    this.fullPathList = fullPathList;
  }

  public String getId() {
    return id;
  }

  public List<String> getFullPathList() {
    return fullPathList;
  }

  public static Function newInstance(String id, List<String> fullPathList) {
    return new Function(id, fullPathList);
  }
}
