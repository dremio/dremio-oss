/*
 * Copyright (C) 2017 Dremio Corporation
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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.util.List;

/**
 * Lists the enums for protostuff based inheritance.
 * generate the class name for a given enum
 *
 */
class SubTypeMapping {

  private final List<Enum<?>> enumContants;
  private final String format;

  public SubTypeMapping(TypesEnum typesEnum) {
    this.format = typesEnum.format();
    this.enumContants = unmodifiableList(asList(typesEnum.types().getEnumConstants()));
  }

  public List<Enum<?>> getEnumConstants() {
    return enumContants;
  }

  public String getFormat() {
    return format;
  }

  public String getClassName(Enum<?> e) {
    String name = e.name();
    String capitalized = name.substring(0, 1).toUpperCase() + name.substring(1);
    return format(format, capitalized);
  }
}
