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
package com.dremio.exec.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

/**
 * RelShuttleImpl that overrides the visitChild().  In Calcite, RelShuttleImpl has a stack, which
 * is used when visitChild() is called.  We do not want our RelShuttleImpl to be stateful as we want
 * static instances to be used.
 */
@SuppressWarnings("checkstyle:regexpsinglelinejava")
public class StatelessRelShuttleImpl extends RelShuttleImpl {
  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    RelNode child2 = child.accept(this);
    if (child2 != child) {
      final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
      newInputs.set(i, child2);
      return parent.copy(parent.getTraitSet(), newInputs);
    }
    return parent;
  }
}
