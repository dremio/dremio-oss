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
package com.dremio.service.autocomplete;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AutocompleteV2RequestType {
  COLUMN("column"),
  CONTAINER("container"),
  REFERENCE("reference");

  private String type;

  private AutocompleteV2RequestType(String type) {
    this.type = type;
  }

  @JsonCreator
  public static AutocompleteV2RequestType fromString(String type) {
    for (AutocompleteV2RequestType requestType : values()) {
      if (requestType.type.equalsIgnoreCase(type)) {
        return requestType;
      }
    }
    throw new IllegalArgumentException(String.format("Unknown enum type %s. Allowed values are %s.", type, Arrays.toString(values())));
  }

  @JsonValue
  public String getType() {
    return type;
  }
}
