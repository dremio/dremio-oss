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
package com.dremio.exec.planner.logical.serialization;

import com.google.common.base.Preconditions;

public class Injection<T> {
  private final Class<T> type;
  private final T value;

  protected Injection(final Class<T> type, final T value) {
    this.type = Preconditions.checkNotNull(type);
    this.value = Preconditions.checkNotNull(value);
  }

  public Class<T> getType() {
    return type;
  }

  public T getValue() {
    return value;
  }

  public static <T> Injection<T> of(final Class<T> klazz, final T value) {
    return new Injection<>(klazz, value);
  }
}