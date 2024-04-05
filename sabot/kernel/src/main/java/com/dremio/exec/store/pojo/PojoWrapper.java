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
package com.dremio.exec.store.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PojoWrapper<T> {
  private final Class<T> pojoClazz;

  private PojoWrapper(Class<T> pojoClazz) {
    this.pojoClazz = pojoClazz;
  }

  /**
   * Creates a PojoWrapper instance for the given class.
   *
   * @param pojoClazz The class of the POJO.
   * @param <T> The type of the POJO.
   * @return A PojoWrapper instance.
   */
  public static <T> PojoWrapper<T> from(Class<T> pojoClazz) {
    return new PojoWrapper<>(pojoClazz);
  }

  /**
   * Returns a list of fields in the given class, that includes the parent class if given. The order
   * is parent class -> child class.
   *
   * @return a list of fields in the given class
   */
  public List<Field> getFields() {
    List<Field> allFields = getAllFields(new ArrayList<>(), pojoClazz);

    return allFields.stream()
        .filter(field -> !Modifier.isStatic(field.getModifiers()))
        .collect(Collectors.toList());
  }

  private List<Field> getAllFields(List<Field> fields, Class<?> type) {
    if (type.getSuperclass() != null) {
      getAllFields(fields, type.getSuperclass());
    }

    fields.addAll(Arrays.asList(type.getDeclaredFields()));

    return fields;
  }
}
