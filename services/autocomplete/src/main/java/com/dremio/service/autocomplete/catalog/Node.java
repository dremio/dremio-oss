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
package com.dremio.service.autocomplete.catalog;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;

public final class Node {
  private final String name;
  private final Type type;

  public Node(String name, Type type) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public enum Type {
    PHYSICAL_SOURCE("Physical Dataset"),
    VIRTUAL_SOURCE("Virtual Dataset"),
    SPACE("Space"),
    FOLDER("Folder"),
    SOURCE("Source"),
    HOME("Home"),
    FILE("File");

    private final String name;

    Type(String name) {
      this.name = name;
    }

    // TODO: This needs to be improved. We shouldn't have json annotations at this level.
    @JsonValue
    public String getName() {
      return name;
    }
  }

  @Override
  public String toString() {
    return "Node{" +
      "name='" + name + '\'' +
      ", type=" + type +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Node node = (Node) o;
    return name.equals(node.name) && type == node.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}
