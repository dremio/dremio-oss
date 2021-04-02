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
package com.dremio.reflection.hints.features;

import java.util.Objects;

import org.apache.calcite.rel.RelNode;

/**
 * Place holder
 */
public class FieldMissingFeature implements HintFeature {
  private final RelNode userQueryNode;
  private final int index;
  private final String name;

  public FieldMissingFeature(RelNode userQueryNode, int index, String name) {
    this.userQueryNode = userQueryNode;
    this.index = index;
    this.name = name;
  }

  public RelNode getUserQueryNode() {
    return userQueryNode;
  }

  public int getIndex() {
    return index;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldMissingFeature that = (FieldMissingFeature) o;
    return index == that.index
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, name);
  }

  @Override
  public String toString() {
    return "FieldMissingFeature{" +
        "index=" + index +
        ", name='" + name + '\'' +
        '}';
  }
}
