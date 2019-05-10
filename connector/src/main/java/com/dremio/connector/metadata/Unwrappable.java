/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.connector.metadata;

/**
 * Objects can be unwrapped.
 */
public interface Unwrappable {

  /**
   * Returns an object that implements the given type/ interface.
   * <p>
   * By default, this object is casted to the given type.
   *
   * @param clazz class that must be implemented
   * @param <T>   type to unwrap to
   * @return object that implements the given type
   * @throws IllegalArgumentException if unwrap fails
   */
  default <T> T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }

    throw new IllegalArgumentException(String.format("This object (type '%s') cannot be unwrapped into '%s'",
        getClass(), clazz));
  }
}
