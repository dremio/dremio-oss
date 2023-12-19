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
package com.dremio.dac.service.autocomplete.model;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SuggestionsType {
  BRANCH("branch"),
  COLUMN("column"),
  CONTAINER("container"),
  REFERENCE("reference"),
  TAG("tag");

  private String type;

  private SuggestionsType(String type) {
    this.type = type;
  }

  @JsonCreator
  public static SuggestionsType fromString(String type) {
    for (SuggestionsType suggestionsType : values()) {
      if (suggestionsType.type.equalsIgnoreCase(type)) {
        return suggestionsType;
      }
    }
    throw new IllegalArgumentException(String.format("Unknown enum type %s. Allowed values are %s.", type, Arrays.toString(values())));
  }

  @JsonValue
  public String getType() {
    return type;
  }
}
