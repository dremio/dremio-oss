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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;

/**
 * RelShuttle, but when it encounters a HepRelVertex it passthrough to the current rel (because
 * whoever designed it didn't make the current rel a child of the node ...)
 */
public class HepRelPassthroughShuttle extends StatelessRelShuttleImpl {
  @Override
  public RelNode visit(RelNode other) {
    if (!(other instanceof HepRelVertex)) {
      return super.visit(other);
    }

    HepRelVertex hepRelVertex = (HepRelVertex) other;
    return hepRelVertex.getCurrentRel().accept(this);
  }
}
