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
package com.dremio.exec.planner.normalizer;

import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import org.apache.calcite.rel.RelNode;

public interface RelNormalizerTransformer {

  PreSerializedQuery transform(RelNode relNode, AttemptObserver attemptObserver)
      throws SqlUnsupportedException;

  /**
   * Normalizes the query with custom logic for reflections.
   *
   * <p>AttemptObserver should be reworked to not require so many dependencies.
   *
   * @param relNode
   * @param relTransformer post-processing for reflections
   * @param attemptObserver
   * @param plannerEventBus
   * @return
   * @throws SqlUnsupportedException
   */
  PreSerializedQuery transformForCompactAndMaterializations(
      RelNode relNode,
      RelTransformer relTransformer,
      AttemptObserver attemptObserver,
      PlannerEventBus plannerEventBus)
      throws SqlUnsupportedException, NormalizerException;

  PreSerializedQuery transformPreSerialization(RelNode relNode);

  RelNode transformPostSerialization(RelNode relNode) throws SqlUnsupportedException;
}
