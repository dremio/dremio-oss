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
package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;

/**
 * RexNamedInputRef represents a RexInputRef, but also stores the name of the corresponding field.
 */
public class RexNamedInputRef extends RexInputRef {

  private final String fieldName;

  /**
   * Creates a named input variable.
   *
   * @param fieldName Name of the input variable
   * @param index Index of the field in the underlying row-type
   * @param type Type of the column
   */
  public RexNamedInputRef(String fieldName, int index, RelDataType type) {
    super(index, type);
    this.fieldName = fieldName;
    this.digest = fieldName;
  }

  /**
   * create a RexNamedInputRef from a RexInputRef.
   *
   * @param rexInputRef Index based input variable
   * @param name Name of the input variable
   * @return a named input variable for the given rexInputRef
   */
  public static RexNamedInputRef of(RexInputRef rexInputRef, String name) {
    return new RexNamedInputRef(name, rexInputRef.getIndex(), rexInputRef.getType());
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj)
        || (obj instanceof RexNamedInputRef
            && fieldName.equals(((RexNamedInputRef) obj).fieldName)
            && index == ((RexNamedInputRef) obj).index);
  }

  @Override
  public int hashCode() {
    return digest.hashCode();
  }
}

// End RexNamedInputRef.java
