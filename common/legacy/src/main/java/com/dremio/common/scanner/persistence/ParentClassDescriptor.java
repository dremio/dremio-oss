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
package com.dremio.common.scanner.persistence;

import static java.util.Collections.unmodifiableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** a parent class and its implementations that was specifically searched for during scanning */
public final class ParentClassDescriptor {
  private final String name;
  private List<ChildClassDescriptor> children;

  @JsonCreator
  public ParentClassDescriptor(
      @JsonProperty("name") String name,
      @JsonProperty("children") List<ChildClassDescriptor> children) {
    this.name = name;
    this.children = unmodifiableList(children);
  }

  /**
   * @return the class name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the implementations
   */
  public List<ChildClassDescriptor> getChildren() {
    return children;
  }

  public void updateChildren(List<ChildClassDescriptor> children) {
    this.children = unmodifiableList(children);
  }

  @Override
  public String toString() {
    return "ParentClassDescriptor [name=" + name + ", children=" + children + "]";
  }
}
