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
package com.dremio.common;

/**
 * The Wrapper pattern allows for more control in complex class inheritance hierarchies or provide access to extensions
 * beyond the current interface. In basic use cases, it can be considered equivalent to `instanceof` and raw casts. For
 * complex hierarchies, implementing classes can choose to perform additional logic when unwrapping ("casting"). For
 * example, to return an intermediate base class.
 *
 * Classes that implement one method must implement the other. Unwrap is expected to succeed if isWrapperFor returns
 * true (and vice versa).
 */
public interface Wrapper {
  /**
   * Returns an object that implements the requested type.
   * <p>
   * By default, this object is casted to the given type.
   * <p>
   * Any class that implements `unwrap` must also implement `isWrapperFor`. Unwrap is expected to succeed if
   * isWrapperFor returns true (and vice versa).
   *
   * @param <T> The type of this implementing class
   * @param clazz The requested type to unwrap this class into
   * @return This object as cast to the requested type
   * @throws IllegalArgumentException if unwrap fails
   */
  default <T> T unwrap(Class<T> clazz) {
    if (isWrapperFor(clazz)) {
      return clazz.cast(this);
    }

    throw new IllegalArgumentException(
      String.format("This object (type '%s') cannot be unwrapped into '%s'", getClass(), clazz));
  }

  /**
   * Checks whether this class can be unwrapped into the requested type. This is intended to be a lightweight operation.
   * <p>
   * By default, checks whether the object is an instance of the requested type.
   * <p>
   * Any class that implements `isWrapperFor` must also implement `unwrap`. Unwrap is expected to succeed if
   * isWrapperFor returns true (and vice versa).
   *
   * @param clazz The requested type to unwrap this class into
   * @return true if this class can be unwrapped into the requested type, false otherwise
   */
  default boolean isWrapperFor(Class<?> clazz) {
    return clazz.isInstance(this);
  }
}
