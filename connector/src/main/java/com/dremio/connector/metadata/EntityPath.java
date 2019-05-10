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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.dremio.connector.Utilities;

/**
 * Entity path. Represents the fully qualified path to the entity.
 */
public class EntityPath {

  private final List<String> components;

  public EntityPath(List<String> components) {
    Utilities.checkNotNullOrEmpty(components);

    this.components = Collections.unmodifiableList(new ArrayList<>(components));
  }

  /**
   * Get the components of the fully qualified path.
   *
   * @return components of the path
   */
  public List<String> getComponents() {
    return components;
  }

  /**
   * Get the size of the fully qualified path.
   *
   * @return path size
   */
  public int size() {
    return components.size();
  }

  /**
   * Get the simple name of the entity.
   *
   * @return entity name
   */
  public String getName() {
    return Utilities.last(components);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof EntityPath)) {
      return false;
    }

    final EntityPath that = (EntityPath) o;
    return components.equals(that.components);
  }

  @Override
  public int hashCode() {
    return components.hashCode();
  }

  @Override
  public String toString() {
    return components.toString();
  }
}
