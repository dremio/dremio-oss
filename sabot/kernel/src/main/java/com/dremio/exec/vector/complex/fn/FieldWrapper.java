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
package com.dremio.exec.vector.complex.fn;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A wrapper class for {@link org.apache.arrow.vector.types.pojo.Field} objects. The purpose is to
 * search for fields based on their case-insensitive name in the most optimal way as possible.
 */
class FieldWrapper {

  private final Field field;
  private final Map<String, FieldWrapper> children;

  /** Convenient implementation to avoid additional checks around this wrapper to avoid NPE. */
  static final FieldWrapper EMPTY =
      new FieldWrapper(Collections.emptyList()) {
        @Override
        public FieldWrapper getListElement() {
          return this;
        }

        @Override
        public FieldWrapper getChild(String name) {
          return this;
        }

        @Override
        public Field getField() {
          throw new NoSuchElementException(
              "getField() should have never been invoked on an EMPTY wrapper");
        }
      };

  FieldWrapper(Field field) {
    this.field = Objects.requireNonNull(field);
    Map<String, FieldWrapper> map = new HashMap<>();
    if (field.getType().getTypeID() == ArrowTypeID.List) {
      assert field.getChildren().size() == 1;
      map.put(null, new FieldWrapper(field.getChildren().get(0)));
    } else {
      for (Field child : field.getChildren()) {
        String name = child.getName();
        map.put(name == null ? null : name.toLowerCase(), new FieldWrapper(child));
      }
    }
    children = map;
  }

  FieldWrapper(Iterable<Field> fields) {
    this.field = null;
    Map<String, FieldWrapper> map = new HashMap<>();
    for (Field child : fields) {
      map.put(child.getName().toLowerCase(), new FieldWrapper(child));
    }
    children = map;
  }

  public FieldWrapper getChild(String name) {
    assert !children.isEmpty();
    return children.computeIfAbsent(name, key -> children.get(key.toLowerCase()));
  }

  public FieldWrapper getListElement() {
    assert field.getType().getTypeID() == ArrowTypeID.List;
    return children.get(null);
  }

  public Field getField() {
    return field;
  }
}
