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
package com.dremio.service.namespace.capabilities;

import com.google.common.base.Objects;

/**
 * Handle for a type of capability.
 */
abstract class Capability<T> {

  private final String name;
  private final T defaultValue;
  private boolean usedInExecution;

  /**
   * Create a capability singleton for reference purposes.
   *
   * @param name
   *          The friendly name of this capability.
   * @param defaultValue
   *          The default value for this capability if it isn't defined by a
   *          source.
   */
  Capability(String name, T defaultValue) {
    super();
    this.name = name;
    this.defaultValue = defaultValue;
  }

  public String getName() {
    return name;
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  public boolean isUsedInExecution() {
    return usedInExecution;
  }

  @Override
  public boolean equals(final Object other) {
    // use class identity since we don't allow capability inheritance beyond this to its direct children.
    if (!(other.getClass().equals(this.getClass()))) {
      return false;
    }
    Capability<?> castOther = (Capability<?>) other;
    return Objects.equal(name, castOther.name) && Objects.equal(defaultValue, castOther.defaultValue)
        && Objects.equal(usedInExecution, castOther.usedInExecution);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, defaultValue, usedInExecution);
  }

}
