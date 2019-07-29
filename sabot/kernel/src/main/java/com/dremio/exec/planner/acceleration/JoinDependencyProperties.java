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
package com.dremio.exec.planner.acceleration;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Metadata used to handle join dependencies analysis
 */
public class JoinDependencyProperties {
  public static final JoinDependencyProperties NONE = new JoinDependencyProperties(ImmutableList.<Dependency>of());

  private List<Dependency> dependencies;

  public JoinDependencyProperties(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }

  public List<Dependency> getDependencies() {
    return dependencies;
  }

  /**
   * A dependency that indicates that a join between the two tables can have the uniqueKeyTable side dropped from
   * the materialization plan.
   */
  public static class Dependency {
    public List<String> foreignKeyTable;
    public List<String> uniqueKeyTable;

    public Dependency(List<String> foreignKeyTable, List<String> uniqueKeyTable) {
      this.foreignKeyTable = foreignKeyTable;
      this.uniqueKeyTable = uniqueKeyTable;
    }

    @Override
    public String toString() {
      return "Dependency{" +
        "foreignKeyTable=" + foreignKeyTable +
        ", uniqueKeyTable=" + uniqueKeyTable +
        '}';
    }
  }
}
