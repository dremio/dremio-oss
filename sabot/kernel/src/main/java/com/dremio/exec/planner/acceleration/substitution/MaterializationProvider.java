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
package com.dremio.exec.planner.acceleration.substitution;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.logical.ViewTable;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;

/**
 * Provides a list of {@link DremioMaterialization} for {@link SubstitutionProvider} Since this
 * provider is used in context of a query, we only want to provide materializations that are
 * actually applicable to that query. Therefore, SubstitutionProvider is expected to first call
 * buildConsideredMaterializations with the user query before calling getConsideredMaterializations.
 */
public interface MaterializationProvider {

  /**
   * Builds list of materializations that overlap with the userQueryNode.
   *
   * @param userQueryNode
   * @return
   */
  List<DremioMaterialization> buildConsideredMaterializations(RelNode userQueryNode);

  /** Returns list of previously built materializations */
  List<DremioMaterialization> getConsideredMaterializations();

  /**
   * Returns the default raw materialization that provider considers for substitution for the VDS
   * with the given path
   *
   * @return The default reflection for the VDS
   */
  Optional<DremioMaterialization> getDefaultRawMaterialization(ViewTable table);

  /** Returns true when materialization cache is ready */
  boolean isMaterializationCacheInitialized();
}
