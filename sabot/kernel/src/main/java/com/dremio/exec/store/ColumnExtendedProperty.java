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

package com.dremio.exec.store;

import com.dremio.connector.metadata.AttributeValue;
import java.util.Objects;

public class ColumnExtendedProperty {
  private final String key;
  private final AttributeValue value;

  public ColumnExtendedProperty(String key, AttributeValue value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public Object getValue() {
    return value.getValueAsObject();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof ColumnExtendedProperty)) {
      return false;
    }

    final ColumnExtendedProperty otherProperty = (ColumnExtendedProperty) other;
    return Objects.equals(key, otherProperty.key) && Objects.equals(value, otherProperty.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}
