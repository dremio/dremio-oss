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

import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.observer.AttemptObserver;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DRRMatcher extends StatelessRelShuttleImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(DRRMatcher.class);

  private final SubstitutionProvider substitutionProvider;
  private final ViewExpansionContext viewExpansionContext;
  private final AttemptObserver attemptObserver;

  private DRRMatcher(
      SubstitutionProvider substitutionProvider,
      ViewExpansionContext viewExpansionContext,
      AttemptObserver attemptObserver) {
    this.substitutionProvider = substitutionProvider;
    this.viewExpansionContext = viewExpansionContext;
    this.attemptObserver = attemptObserver;
  }

  public static RelNode match(
      RelNode relNode,
      SubstitutionProvider substitutionProvider,
      ViewExpansionContext viewExpansionContext,
      AttemptObserver attemptObserver) {
    return relNode.accept(
        new DRRMatcher(substitutionProvider, viewExpansionContext, attemptObserver));
  }

  @Override
  public RelNode visit(RelNode other) {
    if (!(other instanceof ExpansionNode)) {
      return super.visit(other);
    }

    ExpansionNode expansionNode = (ExpansionNode) other;
    Optional<ExpansionNode> optionalDefaultExpansionNode =
        tryGetDefaultExpansionNode(expansionNode);
    if (optionalDefaultExpansionNode.isEmpty()) {
      return super.visit(other);
    }

    return optionalDefaultExpansionNode.get();
  }

  private Optional<ExpansionNode> tryGetDefaultExpansionNode(ExpansionNode expansionNode) {
    ViewTable viewTable = expansionNode.getViewTable();

    // This method also handles the Reflection Hints
    Optional<DremioMaterialization> optionalDremioMaterialization =
        substitutionProvider.getDefaultRawMaterialization(viewTable);
    if (optionalDremioMaterialization.isEmpty()) {
      return Optional.empty();
    }

    DremioMaterialization dremioMaterialization = optionalDremioMaterialization.get();
    Optional<RelNode> relNode =
        substitutionProvider.wrapDefaultExpansionNode(
            expansionNode.getPath(),
            expansionNode,
            dremioMaterialization,
            expansionNode.getRowType(),
            expansionNode.getVersionContext(),
            viewExpansionContext,
            viewTable);

    if (relNode.isEmpty()) {
      LOGGER.info(
          "Unable to get default raw materialization for {}", viewTable.getPath().getSchemaPath());
      return Optional.empty();
    } else {
      ExpansionNode defaultExpansionNode = (ExpansionNode) relNode.get();
      viewExpansionContext.reportDRRSubstitution(
          defaultExpansionNode, dremioMaterialization, attemptObserver);
      return Optional.of(defaultExpansionNode);
    }
  }
}
