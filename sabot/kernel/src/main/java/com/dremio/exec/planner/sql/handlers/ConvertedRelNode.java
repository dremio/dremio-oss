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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.service.namespace.NamespaceKey;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A ConvertedRelNode is the result of converting a SqlNode into a RelNode with {@link
 * org.apache.calcite.plan.Convention#NONE}.
 */
public class ConvertedRelNode {
  private final RelNode relNode;
  private final RelDataType validatedRowType;
  private final List<NamespaceKey> viewIdentifiers;

  private ConvertedRelNode(
      RelNode relNode,
      RelDataType validatedRowType,
      RelNode expandedButNonReducedPlan,
      List<NamespaceKey> viewIdentifiers) {
    this.relNode = relNode;
    this.validatedRowType = validatedRowType;
    this.viewIdentifiers = viewIdentifiers;
  }

  public RelNode getConvertedNode() {
    return this.relNode;
  }

  public RelDataType getValidatedRowType() {
    return this.validatedRowType;
  }

  public List<NamespaceKey> getViewIdentifiers() {
    return viewIdentifiers;
  }

  public static final class Builder {
    private RelNode relNode;
    private RelDataType validatedRowType;
    private RelNode nonReducedRelNode;
    private List<NamespaceKey> viewIdentifiers;

    public Builder withRelNode(RelNode relNode) {
      this.relNode = relNode;
      return this;
    }

    public Builder withValidatedRowType(RelDataType validatedRowType) {
      this.validatedRowType = validatedRowType;
      return this;
    }

    public Builder withViewIdentifiers(List<NamespaceKey> viewIdentifiers) {
      this.viewIdentifiers = viewIdentifiers;
      return this;
    }

    public ConvertedRelNode build() {
      return new ConvertedRelNode(relNode, validatedRowType, nonReducedRelNode, viewIdentifiers);
    }
  }
}
