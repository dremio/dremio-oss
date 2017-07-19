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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

import com.dremio.exec.planner.StatelessRelShuttleImpl;

class MetaDataProviderModifier extends StatelessRelShuttleImpl {
  private final RelMetadataProvider metadataProvider;

  public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  @Override
  public RelNode visit(RelNode other) {
    other.getCluster().setMetadataProvider(metadataProvider);
    return super.visitChildren(other);
  }

  @Override
  public RelNode visit(TableScan scan) {
    scan.getCluster().setMetadataProvider(metadataProvider);
    return super.visit(scan);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    scan.getCluster().setMetadataProvider(metadataProvider);
    return super.visit(scan);
  }

  @Override
  public RelNode visit(LogicalValues values) {
    values.getCluster().setMetadataProvider(metadataProvider);
    return super.visit(values);
  }

  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    child.accept(this);
    parent.getCluster().setMetadataProvider(metadataProvider);
    return parent;
  }
}
