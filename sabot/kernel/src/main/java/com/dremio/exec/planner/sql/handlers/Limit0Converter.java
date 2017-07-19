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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelConversionException;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.EmptyPrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public class Limit0Converter extends BasePrelVisitor<Prel, Void, IOException> {

  private final SqlHandlerConfig config;

  private Limit0Converter(SqlHandlerConfig config) {
    super();
    this.config = config;
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws IOException {
    if (prel instanceof LimitPrel) {
      LimitPrel limit = (LimitPrel) prel;
      if(isLimit0(limit.getFetch())){
        config.getContext().getPlannerSettings().forceSingleMode();
        PhysicalOperator op = PrelTransformer.convertToPop(config, prel);
        BatchSchema schema = op.getSchema(config.getContext().getFunctionRegistry());

        // make sure to remove any selection vector modes since we're now the leaf node.
        schema = schema.clone(SelectionVectorMode.NONE);
        return new EmptyPrel(prel.getCluster(), prel.getTraitSet(), prel.getRowType(), schema);
      }

    }

    List<RelNode> children = new ArrayList<>();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);

  }

  private static boolean isLimit0(RexNode fetch) {
    if (fetch != null && fetch.isA(SqlKind.LITERAL)) {
      RexLiteral l = (RexLiteral) fetch;
      switch (l.getTypeName()) {
      case BIGINT:
      case INTEGER:
      case DECIMAL:
        if (((long) l.getValue2()) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  public static Prel eliminateEmptyTrees(SqlHandlerConfig config, Prel prel) throws RelConversionException {
    final Limit0Converter converter = new Limit0Converter(config);
    try {
      return prel.accept(converter, null);
    }catch (IOException ex){
      throw new RelConversionException("Failure while attempting to convert limit 0 to empty rel.");
    }
  }

}
