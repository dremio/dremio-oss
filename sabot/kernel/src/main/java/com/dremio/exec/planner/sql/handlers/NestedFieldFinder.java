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

import java.util.Locale;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

class NestedFieldFinder extends RelVisitor {

  final RexVisitor<Void> rexFieldAccessVisitor =
    new RexVisitorImpl<Void>(true) {
      @Override
        public Void visitFieldAccess(RexFieldAccess fieldAccess) {
          throw new Util.FoundOne(fieldAccess);
        }

        @Override
        public Void visitCall(RexCall rexCall) {
          if ("dot".equals(rexCall.getOperator().getName().toLowerCase(Locale.ROOT))) {
            throw new Util.FoundOne(rexCall);
          }
          if("item".equals(rexCall.getOperator().getName().toLowerCase(Locale.ROOT))){
            throw new Util.FoundOne(rexCall);
          }
          return super.visitCall(rexCall);
        }

    };


  public boolean run(RelNode input) {
   try {
     go(input);
     return false;
   } catch (Util.FoundOne e) {
     Util.swallow(e, null);
     return true;
   }
  }

  @Override
  public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof Project) {
         ((Project) node).getProjects().forEach(p -> p.accept(rexFieldAccessVisitor));
      }
      if (node instanceof Filter) {
        ((Filter) node).getCondition().accept(rexFieldAccessVisitor);
      }
    super.visit(node, ordinal, parent);
  }


}
